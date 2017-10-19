package GRNOC::Simp::Poller::Worker;

use strict;
use Try::Tiny;
use Data::Dumper;
use Moo;
use AnyEvent;
use AnyEvent::SNMP;
use Net::SNMP;
use Redis;


### required attributes ###

has group_name => (is => 'ro',
		   required => 1);

has instance => (is => 'ro',
		 required => 1);

has config      => ( is => 'ro',
		     required => 1 );


has logger => ( is => 'rwp',
                required => 1 );

has hosts => ( is => 'ro',
               required => 1 );

has oids => ( is => 'ro',
	      required => 1 );

has poll_interval => ( is => 'ro',
		       required => 1 );

has var_hosts => ( is => 'ro',
                   required => 1 );


### internal attributes ###

has worker_name => (is => 'rwp',
		    required => 0,
		    default => 'unknown');

has is_running => ( is => 'rwp',
                    default => 0 );

has need_restart => (is => 'rwp',
		     default => 0 );

has redis => ( is => 'rwp' );

has retention => (is => 'rwp',
		  default => 5);

has max_reps => (is => 'rwp',
		 default => 1);

has snmp_timeout => (is => 'rwp',
		     default => 5);

### public methods ###

sub start {
    
    my ( $self ) = @_;
    
    $self->_set_worker_name($self->group_name . $self->instance);

    my $logger = GRNOC::Log->get_logger($self->worker_name);
    $self->_set_logger($logger);

    my $worker_name = $self->worker_name;
    $self->logger->error( $self->worker_name." Starting." );
    
    # flag that we're running
    $self->_set_is_running( 1 );
    
    # change our process name
    $0 = "simp($worker_name)";

    # setup signal handlers
    $SIG{'TERM'} = sub {
	
        $self->logger->info($self->worker_name. " Received SIG TERM." );
        $self->stop();
    };
    
    $SIG{'HUP'} = sub {
	
        $self->logger->info($self->worker_name. " Received SIG HUP." );
    };

    # connect to redis
    my $redis_host = $self->config->get( '/config/redis/@host' )->[0];
    my $redis_port = $self->config->get( '/config/redis/@port' )->[0];
    
    $self->logger->debug($self->worker_name." Connecting to Redis $redis_host:$redis_port." );
    
    my $redis;

    try {
	#--- try to connect twice per second for 30 seconds, 60 attempts every 500ms.
	$redis = Redis->new(
	    server    => "$redis_host:$redis_port",
	    reconnect => 60,
	    every     => 500,
	    read_timeout => 2,
	    write_timeout => 3,
	    );
    }
    catch {
        $self->logger->error($self->worker_name." Error connecting to Redis: $_" );
    };
    
    $self->_set_redis( $redis );
    
    $self->_set_need_restart(0);
    
    $self->logger->debug( $self->worker_name . ' Starting SNMP Poll loop.' );
    
    $self->_connect_to_snmp();

    $self->{'collector_timer'} = AnyEvent->timer( after => 10,
						  interval => $self->poll_interval,
						  cb => sub {
						      $self->_collect_data();
						  });
    

    #let the magic happen
    AnyEvent->condvar->recv;

}

sub _poll_cb{
    my $self = shift;
    my %params = @_;

    my $session   = $params{'session'};
    my $host      = $params{'host'};
    my $req_time  = $params{'timestamp'};
    my $reqstr    = $params{'reqstr'};
    my $main_oid  = $params{'oid'};
    my $context_id = $params{'context_id'};
    my $redis     = $self->redis;
    my $data      = $session->var_bind_list();

    my $id        = $self->group_name;
    my $timestamp = $req_time;
    my $ip        = $host->{'ip'};


    if(defined($context_id)){
        delete $host->{'pending_replies'}->{$main_oid . "," . $context_id};
    }else{
        delete $host->{'pending_replies'}->{$main_oid};
    }    

    if(!defined $data){
	my $error = $session->error();
	$self->logger->error("Context ID: " . $context_id);
	$self->logger->error("$id failed     $reqstr");
	$self->logger->error("Error: $error");

	return;
    }

    my @values;

    for my $oid (keys %$data){
	push(@values, "$oid," .  $data->{$oid});
    }

    my $expires = $timestamp + $self->retention;

    try {

	$redis->select(0);
	my $key = $host->{'node_name'} . "," . $self->worker_name . ",$timestamp";
	$redis->sadd($key, @values);
	
	if(scalar(keys %{$host->{'pending_replies'}}) == 0){
	    #$self->logger->error("Received all responses!");
	    $redis->expireat($key, $expires);
	    
	    #our poll_id to time lookup
	    $redis->select(1);
	    $redis->set($host->{'node_name'} . "," . $self->group_name . "," . $host->{'poll_id'}, $key);
	    $redis->set($ip . "," . $self->group_name . "," . $host->{'poll_id'}, $key);
	    #and expire
	    $redis->expireat($host->{'node_name'} . "," . $self->group_name . "," . $host->{'poll_id'}, $expires);
	    $redis->expireat($ip . "," . $self->group_name . "," . $host->{'poll_id'},$expires);
	    
            $redis->select(0);
	    #$self->logger->error(Dumper($host->{'group'}{$self->group_name}));

	    if($self->var_hosts()->{$host->{'node_name'}} && defined($host->{'host_variable'})){
		my %add_values = %{$host->{'host_variable'}};
		foreach my $name (keys %add_values){
		    my $sanitized_name = $name;
		    $sanitized_name =~ s/,//g; # we don't allow commas in variable names
		    my $str = 'vars.' . $sanitized_name . "," . $add_values{$name}->{'value'};
		    $redis->select(0);
		    $redis->sadd($key, $str);
		}

		$redis->select(3);
		$redis->sadd($host->{'node_name'}, "vars," . $self->group_name . "," . $self->poll_interval);
		$redis->sadd($ip, "vars," . $self->group_name . "," . $self->poll_interval);
	    }

	    #and the current poll_id lookup
	    $redis->select(2);
	    $redis->set($host->{'node_name'} . "," . $self->group_name, $host->{'poll_id'} . "," . $timestamp);
	    $redis->set($ip . "," . $self->group_name, $host->{'poll_id'} . "," . $timestamp);
	    #and expire
	    $redis->expireat($host->{'node_name'} . "," . $self->group_name, $expires);
	    $redis->expireat($ip . "," . $self->group_name, $expires);

	    $host->{'poll_id'}++;
	}

	
	$redis->select(3);
	$redis->sadd($host->{'node_name'}, $main_oid . "," . $self->group_name . "," . $self->poll_interval);
	$redis->sadd($ip, $main_oid . "," . $self->group_name . "," . $self->poll_interval);

	#change back to the primary db...
	$redis->select(0);
	#complete the transaction
    } catch {
	$redis->select(0);
	$self->logger->error($self->worker_name. " $id Error in hset for data: $_" );
	return;
    }
}

sub _connect_to_snmp{
    my $self = shift;
    my $hosts = $self->hosts;
    foreach my $host(@$hosts){
	# build the SNMP object for each host of interest
	my ($snmp,$error);
	if(!defined($host->{'snmp_version'}) || $host->{'snmp_version'} eq '2c'){
	    
	    ($snmp, $error) = Net::SNMP->session(
		-hostname         => $host->{'ip'},
		-community        => $host->{'community'},
		-version          => 'snmpv2c',
		-timeout          => $self->snmp_timeout,
		-maxmsgsize       => 65535,
		-translate        => [-octetstring => 0],
		-nonblocking      => 1,
	    );

	    if(!defined($snmp)){
		$self->logger->error("Error creating SNMP Session: " . $error);
	    }

	    $self->{'snmp'}{$host->{'ip'}} = $snmp;

	}elsif($host->{'snmp_version'} eq '3'){
	    if(!defined($host->{'group'}{$self->group_name}{'context_id'})){
		($snmp, $error) = Net::SNMP->session(
		    -hostname         => $host->{'ip'},
		    -version          => '3',
		    -timeout          => $self->snmp_timeout,
		    -maxmsgsize       => 65535,
		    -translate        => [-octetstring => 0],
		    -username         => $host->{'username'},
		    -nonblocking      => 1,
		    );
		
		if(!defined($snmp)){
		    $self->logger->error("Error creating SNMP Session: " . $error);
		}
		
		$self->{'snmp'}{$host->{'ip'}} = $snmp;
	    }else{
		foreach my $ctxEngine (@{$host->{'group'}{$self->group_name}{'context_id'}}){
		    ($snmp, $error) = Net::SNMP->session(
			-hostname         => $host->{'ip'},
			-version          => '3',
			-timeout          => $self->snmp_timeout,
			-maxmsgsize       => 65535,
			-translate        => [-octetstring => 0],
			-username         => $host->{'username'},
			-nonblocking      => 1,
			);
		    
		    if(!defined($snmp)){
			$self->logger->error("Error creating SNMP Session: " . $error);
		    }
		    
		    $self->{'snmp'}{$host->{'ip'}}{$ctxEngine} = $snmp;
		}
	    }
	}else{
	    $self->logger->error("Invalid SNMP Version for SIMP");
	}
	
	my $host_poll_id;
	try{
	    $self->redis->select(2);
	    $host_poll_id = $self->redis->get($host->{'node_name'} . ",main_oid");
	    $self->redis->select(0);
	    
	    if(!defined($host_poll_id)){
		$host_poll_id = 0;
	    }

	}catch{
	    $self->redis->select(0);
	    $host_poll_id = 0;
	    $self->logger->error("Error fetching the current poll cycle id from redis: $_");
	};

	$host->{'poll_id'} = $host_poll_id;
	$host->{'pending_replies'} = {};
	
	$self->logger->debug($self->worker_name . " assigned host " . $host->{'ip'});
    }
}

sub _collect_data{
    my $self = shift;

    my $hosts         = $self->hosts;
    my $oids          = $self->oids;
    my $timestamp     = time;
    $self->logger->debug($self->worker_name. " start poll cycle" );

    for my $host (@$hosts){

	if(scalar(keys %{$host->{'pending_replies'}}) > 0){
	    $self->logger->error("Unable to query device " . $host->{'ip'} . ":" . $host->{'name'} . " in poll cycle for group: " . $self->group_name);
	    next;
	}

	for my $oid (@$oids){
	    if(!defined($self->{'snmp'}{$host->{'ip'}})){
		$self->logger->error("No SNMP session defined for " . $host->{'ip'});
		next;
	    }
	    my $reqstr = " $oid -> ".$host->{'ip'};
	    $self->logger->debug($self->worker_name ." requesting ". $reqstr);
	    #--- iterate through the the provided set of base OIDs to collect
	    my $res;

	    if($host->{'snmp_version'} eq '2c'){
		$host->{'pending_replies'}->{$oid} = 1;
		$res = $self->{'snmp'}{$host->{'ip'}}->get_table(
		    -baseoid      => $oid,
		    -maxrepetitions => $self->max_reps,
		    -callback     => sub{ 
			my $session = shift;
			$self->_poll_cb( host => $host,
					 timestamp => $timestamp,
					 reqstr => $reqstr,
					 oid => $oid,
					 session => $session);
		    }
		    );
	    }else{
		#for each context engine specified for the group
		if(!defined($host->{'group'}{$self->group_name}{'context_id'})){
		    $host->{'pending_replies'}->{$oid} = 1;
		    $res = $self->{'snmp'}{$host->{'ip'}}->get_table(
			-baseoid      => $oid,
			-maxrepetitions => $self->max_reps,
			-callback     => sub{
			    my $session = shift;
			    $self->_poll_cb( host => $host,
					     timestamp => $timestamp,
					     reqstr => $reqstr,
					     oid => $oid,
					     session => $session);
			}
			);
		    
		}else{
		    foreach my $ctxEngine (@{$host->{'group'}{$self->group_name}{'context_id'}}){
			$host->{'pending_replies'}->{$oid . "," . $ctxEngine} = 1;
			$res = $self->{'snmp'}{$host->{'ip'}}{$ctxEngine}->get_table(
			    -baseoid      => $oid,
			    -maxrepetitions => $self->max_reps,
			    -contextengineid => $ctxEngine,
			    -callback     => sub{
				my $session = shift;
				$self->_poll_cb( host => $host,
						 timestamp => $timestamp,
						 reqstr => $reqstr,
						 oid => $oid,
						 context_id => $ctxEngine,
						 session => $session);
			    }
			    );
		    }
		}
	    }
	}
    }
}

1;
