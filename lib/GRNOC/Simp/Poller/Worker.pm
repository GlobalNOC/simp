package GRNOC::Simp::Poller::Worker;
#my $worker = GRNOC::Simp::Poller::Worker->new( worker_name => "$name-$worker_id",
#						config      => $config,
#                                               oids        => $oids,
#                                               hosts       => $hosts{$worker_id},
#                                               logger      => $self->logger);

use strict;
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

    #try {
        $redis = Redis->new(server => '127.0.0.1:6379');

        #$redis = Redis->new( server => "$redis_host:$redis_port" );
    #}

    #catch {
    #    print Dumper($@);
    #    $self->logger->error( "Error connecting to Redis: $_" );
    #    die( "Error connecting to Redis: $_" );
    #};

    $self->_set_redis( $redis );

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

  my $redis     = $self->redis;
  my $data      = $session->var_bind_list();

  my $id        = $self->worker_name;
  my $timestamp = time;
  my $ip        = $host->{'ip'};

  if(!defined $data){
    my $error = $session->error();
    $self->logger->debug("$id rx fail $error");
    return;
  }
  $self->logger->debug("$id rx ok $ip");

  for my $oid (keys %$data){
    #--- use a compound key IP, PollerID and Request Timestamp. 
    #--- Request Timestamp used so that all oids requests for a node are keyed the same
    my $key = "$ip,$id,$req_time";
    $redis->hset($oid,$key,$data->{$oid},sub {});

    #--- track what we have in redis so we can expire later. 
    $self->{'cacheEntries'}{$req_time}{$oid}{$ip} = $key;

  }

  #--- last update records stored in database index "1" vs "0" for data
  #--- track last seen timestamp on per host per worker to account for different parallel poll cycles
  $redis->select(1);
  $redis->hset($ip,$id,$timestamp,sub {});
  $redis->select(0);

  $last_seen->{$ip} = $timestamp;
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
    ($cursor,$oids) =  $redis->scan($cursor,COUNT=>200);

    foreach my $oid (@$oids){
      #--- now for each OID entry pull the set of compond keys that include a ts,ip
      my $keys = $redis->hkeys($oid);

      foreach my $key (@$keys){
	my ($ip,$id,$ts) = split(/,/,$key);
        next if(!($id eq $self->worker_name)  || ! defined $ip);
	#$self->logger->debug($self->worker_name . " readd $ts : $oid : $ip\n" );
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

    $self->logger->debug($self->worker_name .  " purge cache ts $ts" );
    foreach my $oid (keys %{$self->{'cacheEntries'}{$ts}}){
      foreach my $ip (keys %{$self->{'cacheEntries'}{$ts}{$oid}}){
        my $key = $self->{'cacheEntries'}{$ts}{$oid}{$ip};
        #--- delete the entry from redis
        $redis->hdel($oid,$key,sub {});  
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
	 $self->logger->debug($self->worker_name . " request $oid from ".$host->{'ip'}."\n");
        #--- iterate through the the provided set of base OIDs to collect
        my $res =  $host->{'snmp'}->get_table( 
          -baseoid      => $oid ,
          -callback     => [\&_poll_callback,$self,$host,$timestamp,\%last_seen],
        );
      } 
    }
  
    #--- go into reactive phase  wait for all results to return or timeouts
    snmp_dispatcher();

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
