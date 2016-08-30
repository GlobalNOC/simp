package GRNOC::Simp::Data::Worker;

use strict;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Moo;
use Redis;
use GRNOC::RabbitMQ::Method;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::WebService::Regex;
use Net::SNMP;

### required attributes ###

has config => ( is => 'ro',
                required => 1 );

has logger => ( is => 'ro',
                required => 1 );

has worker_id => ( is => 'ro',
               required => 1 );

### internal attributes ###

has is_running => ( is => 'rwp',
                    default => 0 );

has redis => ( is => 'rwp' );


 ### public methods ###

sub start {

    my ( $self ) = @_;

    my $worker_id = $self->worker_id;

    $self->logger->debug( "Starting." );

    # flag that we're running
    $self->_set_is_running( 1 );

    # change our process name
    $0 = "simp_data ($worker_id) [worker]";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( "Received SIG TERM." );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    my $redis_host = $self->config->get( '/config/redis/@host' );
    my $redis_port = $self->config->get( '/config/redis/@port' );
  
    my $rabbit_host = $self->config->get( '/config/rabbitMQ/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbitMQ/@port' );
    my $rabbit_user = $self->config->get( '/config/rabbitMQ/@user' );
    my $rabbit_pass = $self->config->get( '/config/rabbitMQ/@password' );
 
   
    # conect to redis
    $self->logger->debug( "Connecting to Redis $redis_host:$redis_port." );

    my $redis;

    $redis = Redis->new(server => '127.0.0.1:6379');

    $self->_set_redis( $redis );

    $self->logger->debug( 'Starting rabbit event loop.' );

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new( 	queue => "Simp",
							topic => "Simp.Data",
							exchange => "Simp",
							user => $rabbit_user,
							pass => $rabbit_pass,
							host => $rabbit_host,
							port => $rabbit_port);

    my $method = GRNOC::RabbitMQ::Method->new(	name => "get",
						callback =>  sub { $self->_get2(@_) },
						description => "function to pull SNMP data out of cache");

    $method->add_input_parameter( name => "oidmatch",
                                  description => "redis pattern for specifying the OIDS of interest",
                                  required => 1,
                                  pattern => $GRNOC::WebService::Regex::TEXT);


    $method->add_input_parameter( name => "ipaddrs",
                                  description => "array of ip addresses to fetch data for",
                                  required => 1,
                                  schema => { 'type'  => 'array',
                                              'items' => [ 'type' => 'string',
                                                         ]
                                            } );

    $dispatcher->register_method($method);

    my $method2 = GRNOC::RabbitMQ::Method->new(  name => "ping",
                                                callback =>  sub { $self->_ping($dispatcher) },
                                                description => "function to test latency");

    $dispatcher->register_method($method2);
    
    print Dumper($AnyEvent::MODEL);

    # where the magic happens
    return $dispatcher->start_consuming();
}

### private methods ###

sub _ping{
  my $self = shift;
  return gettimeofday();
}


sub _scan_cb{
  my $self     = shift;
  my $ipaddrs  = shift;
  my $ref      = shift;
  my $reply    = shift;
  my $erro     = shift;

  my $redis    = $self->redis;
 
  foreach my $key (@$reply){
    my $vals =$redis->hmget($key,@$ipaddrs,sub {$self->_hmget_cb($ipaddrs,$key,$ref,@_);});
  } 
}

sub _hmget_cb{
  my $self      = shift;
  my $ipaddrs   = shift;
  my $key       = shift;
  my $ref       = shift;
  my $reply     = shift;
  my $error     = shift;

  my $redis    = $self->redis;  

  foreach my $ip (@$ipaddrs){
    my $val = shift @$reply;
    next if(!defined $val);         #-- this OID key has no relevance to the IP in question if null here
    $ref->{$ip}{$key} = $val;       #-- external data representation is inverted from what we store
  }
}



sub _get2{
  #--- implementation using pipelining and callbacks 
  my ($self,$method_ref,$params) = @_;
  my $ipaddrs  = $params->{'ipaddrs'}{'value'};
  my $oidmatch = $params->{'oidmatch'}{'value'};

  my $redis    = $self->redis;

  my $cursor = 0;
  my $keys;
  my %results;

  while(1){
    ($cursor,$keys) =  $redis->scan($cursor,MATCH=>$oidmatch,COUNT=>200);
    $self->_scan_cb($ipaddrs,\%results,$keys);
    last if($cursor == 0);
  }
  $redis->wait_all_responses;
  return \%results;
}



sub _get{
  #--- blocking implementation is about 33% slower than _get2
  my ($self,$method_ref,$params) = @_;
  my $ipaddrs  = $params->{'ipaddrs'}{'value'};
  my $oidmatch = $params->{'oidmatch'}{'value'};

  my $redis    = $self->redis;

  my $cursor = 0;
  my $keys;
  my %results;

  while(1){
    ($cursor,$keys) = $redis->scan($cursor,MATCH=>$oidmatch,COUNT=>200);
    foreach my $key (@$keys){
      my $vals =$redis->hmget($key,@$ipaddrs);
      next if(!defined $vals);
      foreach my $ip (@$ipaddrs){
        my $val = shift @$vals;
	next if(!defined $val);		#-- this OID key has no relevance to the IP in question if null here
        $results{$ip}{$key} = $val;
      }
    }
    last if($cursor == 0);
  }
  return \%results;
}

1;
