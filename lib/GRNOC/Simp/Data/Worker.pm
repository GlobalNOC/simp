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
						callback =>  sub { $self->_get(@_) },
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
    
    # where the magic happens
    return $dispatcher->start_consuming();
}

### private methods ###

sub _ping{
  my $self = shift;
  return gettimeofday();
}

#--- calllback function to process results when building our hostkey list
sub _hostkey_cb{
  my $self      = shift;
  my $ip        = shift;
  my $ref       = shift;
  my $reply     = shift; 

  while(1){
    my $key = shift @$reply;
    my $val = shift @$reply;
    last if(! defined $key || ! defined $val);

    #--- build the host key and add to the array of keys
    push(@$ref, "$ip,$key,$val");


  }

}


#--- returns a hash that maps ip to the host key
sub _gen_hostkeys{
  my $self      = shift;
  my $ipaddrs   = shift;
  my $redis     = $self->redis;

  my @results;

  #--- the timestamps are kept in a different db "1" vs "0"
  $redis->select(1);
  
  foreach my $ip (@$ipaddrs){
     my $vals =$redis->hgetall($ip, sub {$self->_hostkey_cb($ip,\@results,@_);} );
    
  }

  #--- wait for all the hgetall responses to return
  $redis->wait_all_responses;

  #--- return back to db 0
  $redis->select(0);
  return \@results;
}


#--- callback to handle results from the hmgets issued in _get
sub _get_cb{
  my $self      = shift;
  my $hkeys     = shift;
  my $key       = shift;
  my $ref       = shift;
  my $reply     = shift;
  my $error     = shift;

  my $redis    = $self->redis;  

  foreach my $hk (@$hkeys){
    #--- Host Ip, Collector ID, TimeStamp
    my ($ip,$id,$ts) = split(/,/,$hk);
    my $val = shift @$reply;
    next if(!defined $val);         #-- this OID key has no relevance to the IP in question if null here
    $ref->{$ip}{$key} = $val;       #-- external data representation is inverted from what we store
  }
}


sub _get{
  #--- implementation using pipelining and callbacks 
  my ($self,$method_ref,$params) = @_;
  my $ipaddrs  = $params->{'ipaddrs'}{'value'};
  my $oidmatch = $params->{'oidmatch'}{'value'};

  my $redis    = $self->redis;

  my $cursor = 0;
  my $keys;
  my %results;

  
  #--- convert the set of interesting ip address to the set of internal keys used to retrive data 
  my $hkeys = $self->_gen_hostkeys($ipaddrs);

  while(1){
    #---- get the set of hash entries that match our pattern
    ($cursor,$keys) =  $redis->scan($cursor,MATCH=>$oidmatch,COUNT=>200);
  
    foreach my $key (@$keys){
      #--- iterate on the returned OIDs and pull the values associated to each host
      my $vals =$redis->hmget($key,@$hkeys,sub {$self->_get_cb($hkeys,$key,\%results,@_);});
    } 
    last if($cursor == 0);
  }

  #--- wait for all pending responses to hmget requests
  $redis->wait_all_responses;

  return \%results;
}

1;
