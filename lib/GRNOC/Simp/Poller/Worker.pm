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

has worker_name => ( is => 'ro',
                required => 1 );

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


### internal attributes ###

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
    
    my $logger = GRNOC::Log->get_logger($self->worker_name);
    $self->_set_logger($logger);
    my $worker_name = $self->worker_name;
    
    $self->logger->debug( $self->worker_name." Starting." );
    
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

    my $redis     = $self->redis;
    my $data      = $session->var_bind_list();

    my $id        = $self->worker_name;
    my $timestamp = $req_time;
    my $ip        = $host->{'ip'};
    

    if(!defined $data){
	my $error = $session->error();
	$self->logger->error("$id failed     $reqstr");
	$self->logger->error("Error: $error");
	return;
    }

    for my $oid (keys %$data){
	try {
	    my $key = "$ip,$id,$main_oid,$timestamp";
	    $redis->hset($oid,$key,$data->{$oid});
	    
	    if(defined($host->{'node_name'})){
		my $node_name = $host->{'node_name'};
		
		#also push into node_name
		my $key2 = "$node_name,$id,$main_oid,$timestamp";
		$redis->hset($oid,$key2,$data->{$oid});
	    }
	} catch {
	    $self->logger->error($self->worker_name. " $id Error in hset for data: $_" );
	    #--- on error try to restart
	    return;
	};
    }
	
    #--- last update records stored in database index "1" vs "0" for data
    #--- track last seen timestamp on per host per worker to account for different parallel poll cycles
    try{
	$redis->select(1);
	$redis->lpush("$ip,$id,$main_oid",$timestamp);
	if(defined($host->{'node_name'})){
	    my $node_name = $host->{'node_name'};
	    $redis->lpush("$node_name,$id,$main_oid", $timestamp);
	}
	$redis->select(0);
    } catch {
	$self->logger->error($self->worker_name. " $id Error in hset for timestamp: $_" );
	#--- on error try to restart
	$redis->select(0);
	return;
    };
}

### privatge methods ###
sub _poll_callback{
  my $session   = shift;
  my $self      = shift;
  my $host      = shift;
  my $req_time  = shift;
  my $last_seen = shift;
  my $reqstr    = shift;
  my $main_oid  = shift;

  my $redis     = $self->redis;
  my $data      = $session->var_bind_list();

  my $id        = $self->worker_name;
  my $timestamp = time;
  my $ip        = $host->{'ip'};

  if(!defined $data){
    my $error = $session->error();
    $self->logger->debug("$id failed     $reqstr");
    return;
  }
  $self->logger->debug("$id recieved   $reqstr");
  $last_seen->{$ip} = $timestamp;

  for my $oid (keys %$data){
    #--- use a compound key IP, PollerID and Request Timestamp. 
    #--- Request Timestamp used so that all oids requests for a node are keyed the same
    try {  
      my $key = "$ip,$id,$main_oid,$timestamp";
      $redis->hset($oid,$key,$data->{$oid});
     
      if(defined($host->{'node_name'})){
	  my $node_name = $host->{'node_name'};
	  
	  #also push into node_name
	  my $key2 = "$node_name,$id,$main_oid,$timestamp";
	  $redis->hset($oid,$key2,$data->{$oid});
      }
    } catch {
        $self->logger->error($self->worker_name. " $id Error in hset for data: $_" );
        #--- on error try to restart
        $self->_set_need_restart(1);
        return;
    };

  }

  #--- last update records stored in database index "1" vs "0" for data
  #--- track last seen timestamp on per host per worker to account for different parallel poll cycles
  try{  
    $redis->select(1);
    $redis->lpush("$ip,$id,$main_oid",$timestamp);
    if(defined($host->{'node_name'})){
	my $node_name = $host->{'node_name'};
	$redis->lpush("$node_name,$id,$main_oid", $timestamp);
    }
    $redis->select(0);
  } catch {
    $self->logger->error($self->worker_name. " $id Error in hset for timestamp: $_" );
    #--- on error try to restart
    $self->_set_need_restart(1);
    return;
  };
 
}


sub _connect_to_snmp{
    my $self = shift;
    my $hosts = $self->hosts;
    foreach my $host(@$hosts){
	# build the SNMP object for each host of interest
	my ($snmp, $error) = Net::SNMP->session(
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
	for my $oid (@$oids){
	    if(!defined($self->{'snmp'}{$host->{'ip'}})){
		$self->logger->error("No SNMP session defined for " . $host->{'ip'});
		next;
	    }
	    my $reqstr = " $oid -> ".$host->{'ip'};
	    $self->logger->debug($self->worker_name ." requesting ". $reqstr);
	    #--- iterate through the the provided set of base OIDs to collect
	    my $res =  $self->{'snmp'}{$host->{'ip'}}->get_table(
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
	}
    }
}

1;
