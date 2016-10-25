package GRNOC::Simp::Poller::Worker;
#my $worker = GRNOC::Simp::Poller::Worker->new( worker_name => "$name-$worker_id",
#						config      => $config,
#                                               oids        => $oids,
#                                               hosts       => $hosts{$worker_id},
#                                               logger      => $self->logger);

use strict;
use Try::Tiny;
use Data::Dumper;
use Moo;
use Redis;
use Net::SNMP;

### required attributes ###

has worker_name => ( is => 'ro',
                required => 1 );

has config      => ( is => 'ro',
               required => 1 );


has logger => ( is => 'ro',
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


 ### public methods ###

sub start {

    my ( $self ) = @_;

    my $worker_name = $self->worker_name;

    $self->logger->debug( "Starting." );

    # flag that we're running
    $self->_set_is_running( 1 );

    # change our process name
    $0 = "simp($worker_name)";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( "Received SIG TERM." );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    # connect to redis
    my $redis_host = $self->config->get( '/config/redis/@host' )->[0];
    my $redis_port = $self->config->get( '/config/redis/@port' )->[0];

    $self->logger->debug( "Connecting to Redis $redis_host:$redis_port." );

    my $redis;

    try {
      #--- try to connect twice per second for 30 seconds, 60 attempts every 500ms.
      $redis = Redis->new(
				server 	  => "$redis_host:$redis_port",
				reconnect => 60, 
				every     => 500,
				read_timeout => .2,
				write_timeout => .3,
			);
    }
    catch {
        $self->logger->error( "Error connecting to Redis: $_" );
        #--- on error try to restart
        sleep 10;
        $self->start();
    };

    $self->_set_redis( $redis );

    $self->_set_need_restart(0);

    $self->logger->debug( 'Starting SNMP Poll loop.' );

    # where the magic happens
    return $self->_poll_loop();
}

### privatge methods ###
sub _poll_callback{
  my $session   = shift;
  my $self      = shift;
  my $host      = shift;
  my $req_time  = shift;
  my $last_seen = shift;
  my $reqstr    = shift;

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
      my $key = "$ip,$id,$req_time";
      $redis->hset($oid,$key,$data->{$oid},sub {});

      #--- track what we have in redis so we can expire later. 
      $self->{'cacheEntries'}{$req_time}{$oid}{$ip} = $key;
    } catch {
        $self->logger->error( "$id Error in hset for data: $_" );
        #--- on error try to restart
        $self->_set_need_restart(1);
        return;
    };

  }

  #--- last update records stored in database index "1" vs "0" for data
  #--- track last seen timestamp on per host per worker to account for different parallel poll cycles
  try{  
    $redis->select(1);
    $redis->hset($ip,$id,$timestamp,sub {});
    $redis->select(0);
  } catch {
    $self->logger->error( "$id Error in hset for timestamp: $_" );
    #--- on error try to restart
    $self->_set_need_restart(1);
    return;
  };
 
}

sub _rebuild_cache{
  my $self   = shift;
  my $redis  = $self->redis;

  my $cursor = 0;
  my $oids;
  my $x =0;

  #--- scan the set of records in redis
  while(1){
    #---- get the set of hash entries that match our pattern
    try { 
      ($cursor,$oids) =  $redis->scan($cursor,COUNT=>200);
    } catch {
      $self->logger->error( $self->workder_name." Error rebuilding cache in scan: $_" );
      #--- on error try to restart
      $self->_set_need_restart(1);
      return;
    };

    $redis->select(0);
    foreach my $oid (@$oids){
      #--- now for each OID entry pull the set of compond keys that include a ts,ip
      my $keys;
      try{
        $keys = $redis->hkeys($oid);
      } catch {
        $self->logger->error( $self->workder_name." Error rebuilding cache using hkeys: $_" );
        #--- on error try to restart
        $self->_set_need_restart(1);
        return;
      };

      foreach my $key (@$keys){
	my ($ip,$id,$ts) = split(/,/,$key);
        next if(!defined $id || !($id eq $self->worker_name)  || ! defined $ip);
        $self->{'cacheEntries'}{$ts}{$oid}{$ip} = $key;    
        $x++;   

      }
    }
    last if($cursor == 0);
  }
  $self->logger->debug($self->worker_name. " cache rebuilt: added $x entries" );

}



sub _purge_data{
  my $self    = shift;
  my $thresh  = shift;

  my $redis   = $self->redis;

  #--- the entries in the cache are local to each worker
  foreach my $ts (keys %{$self->{'cacheEntries'}}){
    next if($ts > $thresh);   #--ignore any entries that are too new

    my $str = gmtime($ts);
    $self->logger->debug($self->worker_name .  " purge cache ts: $str" );
    foreach my $oid (keys %{$self->{'cacheEntries'}{$ts}}){
      foreach my $ip (keys %{$self->{'cacheEntries'}{$ts}{$oid}}){
        my $key = $self->{'cacheEntries'}{$ts}{$oid}{$ip};
        #--- delete the entry from redis
        try {
          $redis->hdel($oid,$key,sub {});  
        } catch {
          $self->logger->error( "Error purging entries using hdel: $_" );
          #--- on error try to restart
          $self->start();
        };
 
      }
    }
    #--- delete the from local cache
    delete $self->{'cacheEntries'}{$ts};
  }
}


sub _poll_loop {

  my ( $self ) = @_;

  my $poll_interval = $self->poll_interval;
  my $hosts         = $self->hosts;
  my $oids          = $self->oids;

  my %last_seen;

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
  $self->_rebuild_cache();


  while ( 1 ) {
    my $timestamp = time;
    my $waketime = $timestamp + $poll_interval;


    #--- purge data that is older than 2x poll_interval
    my $threshold = $timestamp - (2*$poll_interval);
    $self->_purge_data($threshold);   

    $self->logger->debug($self->worker_name. " start poll cycle" ); 
    for my $host (@$hosts){ 
      for my $oid (@$oids){
	 my $reqstr = " $oid -> ".$host->{'ip'};
	 $self->logger->debug($self->worker_name ." requesting ". $reqstr);
        #--- iterate through the the provided set of base OIDs to collect
        my $res =  $host->{'snmp'}->get_table( 
          -baseoid      => $oid ,
          -callback     => [\&_poll_callback,$self,$host,$timestamp,\%last_seen,$reqstr],
        );
      } 
    }
  
    #--- go into reactive phase  wait for all results to return or timeouts
    snmp_dispatcher();

    #--- check to see if we need to restart
    if($self->need_restart()){
      $self->start();
    }

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
