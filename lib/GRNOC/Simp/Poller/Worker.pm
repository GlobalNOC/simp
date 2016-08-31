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
    my $redis_host = $self->config->get( '/config/redis/@host' );
    my $redis_port = $self->config->get( '/config/redis/@port' );

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
  my $last_seen = shift;

  my $redis     = $self->redis;
  my $data      = $session->var_bind_list();

  my $id      = $self->worker_id;

  if(!defined $data){
    my $error = $session->error();
    $self->logger->debug("wkr $id rx fail $error");
    return;
  }
  $self->logger->debug("wkr $id rx ok ".$host->{'ip'});

  # record when time was received
  my $timestamp = time;

  for my $oid (keys %$data){
    $redis->hset($oid,$host->{'ip'},$data->{$oid},sub {});
  }
  $redis->hset("ts",$host->{'ip'},$timestamp,sub {});

  $last_seen->{$host->{'ip'}} = $timestamp;

}

sub _poll_loop {

  my ( $self ) = @_;

  my $poll_interval = $self->poll_interval;
  my $hosts         = $self->hosts;
  my $oids          = $self->oids;

  # each worker looks at every Nth host entry in the config
  # where N is the worker_id
  my @target_hosts;

  my $size = scalar @{ $hosts} ;
  
  my %last_seen;
  
  foreach my $host(@{$self->hosts}){
      
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
      push(@target_hosts,$host);
  }

  while ( 1 ) {
    my $timestamp = time;
    my $waketime = $timestamp + $poll_interval;

    $self->logger->debug($self->worker_name. " start poll cycle" ); 
    for my $host (@target_hosts){ 
      for my $oid (@$oids){
        #--- iterate through the the provided set of base OIDs to collect
        my $res =  $host->{'snmp'}->get_table( 
          -baseoid      => $oid ,
          -callback     => [\&_poll_callback,$self,$host,\%last_seen],
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
