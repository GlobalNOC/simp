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

    $self->{'purge_timer'} = AnyEvent->timer( after => 1, 
					      interval => $self->poll_interval * 2,
					      cb => sub {
						  $self->_purge_data();
					      });
    
    $self->{'collector_timer'} = AnyEvent->timer( after => 1,
						  interval => $self->poll_interval,
						  cb => sub {
						      $self->_collect_data();
						  });
    

    #let the magic happen
    AnyEvent->condvar->recv;

    # where the magic happens
#    return $self->_poll_loop();
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
    my $timestamp = time;
    my $ip        = $host->{'ip'};


    if(!defined $data){
	my $error = $session->error();
	$self->logger->debug("$id failed     $reqstr");
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


sub _purge_data{
  my $self    = shift;

  my $redis   = $self->redis;
  my $id      = $self->worker_name;  

  $self->logger->error("$id Starting Purge of stale data");

  return if(!defined $self->hosts);

  my @to_be_removed;
  my @ready_for_delete;
  foreach my $host(@{$self->hosts}){
      foreach my $oid (@{$self->oids}){
	  my $key = $host->{'ip'} . "," . $id . "," . $oid;
	  
	  $redis->select(1);

	  while( $redis->llen($key) > $self->retention){
	      my $ts = $redis->rpop($key);
	      push(@to_be_removed, $key . ",$ts");
	  }
	  
	  if(defined($host->{'node_name'})){
	      my $node_key = $host->{'node_name'} . "," . $id . "," . $oid;
	      
	      while( $redis->llen($node_key) > $self->retention ){
		  my $ts = $redis->rpop($node_key);
		  push(@to_be_removed, $node_key . ",$ts");
	      }
	  }	  
      }
  }
  #$self->logger->error("Have a list of stale entries");
  
  $redis->select(0);
  
  my @possible_oids;
  #find all the possible OIDs!
  my $cv = AnyEvent->condvar;
  foreach my $oid (@{$self->oids}){
      $cv->begin;
      my $cursor = 0;
      while(1){
	  my $keys;
	  my $oid_search = $oid . ".*";
	  #---- get the set of hash entries that match our pattern
	  my $cv = AnyEvent->condvar;
	  ($cursor,$keys) = $redis->scan($cursor, MATCH => $oid_search, COUNT => 2000);
	  foreach my $key (@$keys){
	      push(@possible_oids,$key);
	  }
	  last if($cursor == 0);
      }
  }

  my $removed = 0;
  
  if(scalar(@to_be_removed) >= 1){
      foreach my $key (@possible_oids){
	  my $res = $redis->hdel($key, @to_be_removed);
	  $removed += $res;
      }
  }
  
  warn "Total Removed: " . $removed . "\n";  
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
	    -maxmsgsize       => 65535,
	    -translate        => [-octetstring => 0],
	    -nonblocking      => 1,
	    );
	
	$self->{'snmp'}{$host->{'ip'}} = $snmp;
	
#	my $ip = $host->{'ip'};
#	$last_seen{$ip} = time;
	
	$self->logger->debug($self->worker_name . " assigned host " . $host->{'ip'});
    }
}

sub _collect_data{
    my $self = shift;

    my $hosts         = $self->hosts;
    my $oids          = $self->oids;
    my $timestamp     = time;
    $self->logger->error($self->worker_name. " start poll cycle" );

    for my $host (@$hosts){
	for my $oid (@$oids){
	    my $reqstr = " $oid -> ".$host->{'ip'};
	    $self->logger->debug($self->worker_name ." requesting ". $reqstr);
	    #--- iterate through the the provided set of base OIDs to collect
	    my $res =  $self->{'snmp'}{$host->{'ip'}}->get_table(
		-baseoid      => $oid,
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

sub _poll_loop {

  my ( $self ) = @_;

  my $poll_interval = $self->poll_interval;
  my $hosts         = $self->hosts;
  my $oids          = $self->oids;

  my %last_seen;

  #--- if there are no hosts, there is nothing to do
  if(!defined $hosts || scalar @$hosts == 0){
    $self->logger->warn($self->worker_name. " no hosts found, nothing to do");
    return; 
  }

  foreach my $host(@$hosts){
     # build the SNMP object for each host of interest
     my ($snmp, $error) = Net::SNMP->session(
        -hostname         => $host->{'ip'},
        -community        => $host->{'community'},
        -version          => 'snmpv2c',
        -maxmsgsize       => 65535,
        -translate        => [-octetstring => 0],
        -nonblocking      => 1,
      );
      $host->{'snmp'} = $snmp; 

      my $ip = $host->{'ip'};
      $last_seen{$ip} = time;   

      $self->logger->debug($self->worker_name . " assigned host $ip ");
  }

  #--- on a restart we have lost our in memory cache but the data is still in redis
  #--- need to perform a scan to rebuild cache

  while (1) {
    my $timestamp = time;
    my $waketime = $timestamp + $poll_interval;

    #--- purge data that is older than 2x poll_interval
    $self->_purge_data();   

    $self->logger->error($self->worker_name. " start poll cycle" ); 
    for my $host (@$hosts){ 
      for my $oid (@$oids){
	 my $reqstr = " $oid -> ".$host->{'ip'};
	 $self->logger->debug($self->worker_name ." requesting ". $reqstr);
        #--- iterate through the the provided set of base OIDs to collect
        my $res =  $host->{'snmp'}->get_table( 
          -baseoid      => $oid ,
          -callback     => [\&_poll_callback,$self,$host,$timestamp,\%last_seen,$reqstr, $oid],
        );
      } 
    }
  
    #--- go into reactive phase  wait for all results to return or timeouts
    snmp_dispatcher();

    #--- check to see if we need to restart
#    if($self->need_restart()){
#	$self->logger->error("Trying to restart!");
#      $self->start();
#    }

    #--- check if we have any hosts that have gone unresponsive
    foreach my $ip (keys %last_seen){
      my $lastup = $last_seen{$ip};
      if($lastup < time - (2 *$poll_interval)){
        #--- no response in last poll cycle 
        $self->logger->warn($self->worker_name. " no data from $ip in 3 poll cycles");
        delete $last_seen{$ip};
      }

    }

    sleep($waketime - time);
   } 
}
1;
