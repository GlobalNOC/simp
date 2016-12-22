package GRNOC::Simp::Data::Worker;

use strict;
use Carp;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Try::Tiny;
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

has dispatcher  => ( is => 'rwp' );

has need_restart => (is => 'rwp',
                    default => 0 );


### public methods ###
sub start {
   my ( $self ) = @_;

  while(1){
    #--- we use try catch to, react to issues such as com failure
    #--- when any error condition is found, the reactor stops and we then reinitialize 
    $self->logger->debug( $self->worker_id." restarting." );
    $self->_start();
    sleep 2;
  }

}

sub _start {

    my ( $self ) = @_;

    my $worker_id = $self->worker_id;

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

    
    #--- try to connect twice per second for 30 seconds, 60 attempts every 500ms.
    $redis = Redis->new(
                                server    => "$redis_host:$redis_port",
                                reconnect => 60,
                                every     => 500,
                                read_timeout => .2,
                                write_timeout => .3,
                        );

    $self->_set_redis( $redis );

    $self->logger->debug( 'Setup RabbitMQ' );

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new( 	queue => "Simp",
							topic => "Simp.Data",
							exchange => "Simp",
							user => $rabbit_user,
							pass => $rabbit_pass,
							host => $rabbit_host,
							port => $rabbit_port);


    my $method = GRNOC::RabbitMQ::Method->new(	name => "get",
						callback =>  sub {
								   my ($method_ref,$params) = @_; 
								   return $self->_get(0,$params); 
								 },
						description => "function to pull SNMP data out of cache");

    $method->add_input_parameter( name => "oidmatch",
                                  description => "redis pattern for specifying the OIDS of interest",
                                  required => 1,
                                  multiple => 1,
                                  pattern => $GRNOC::WebService::Regex::TEXT);


    $method->add_input_parameter( name => "ipaddrs",
                                  description => "array of ip addresses to fetch data for",
                                  required => 1,
                                  schema => { 'type'  => 'array',
                                              'items' => [ 'type' => 'string',
                                                         ]
                                            } );

    $dispatcher->register_method($method);

    $method = GRNOC::RabbitMQ::Method->new(  name => "get_rate",
                                             callback =>  sub {
							        my ($method_ref,$params) = @_; 
								return $self->_get_rate($params);
							  },
                                             description => "function to pull SNMP data out of cache and calculate the rate");
    
    $method->add_input_parameter( name => "oidmatch",
                                  description => "redis pattern for specifying the OIDS of interest",
                                  required => 1,
                                  multiple => 1,
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
    
    #--- go into event loop handing requests that come in over rabbit  
    $self->logger->debug( 'Entering RabbitMQ event loop' );
    $dispatcher->start_consuming();
    
    #--- you end up here if one of the handlers called stop_consuming
    #--- this is done when there are internal issues getting to redis that require a re-init.
    return;
}

### private methods ###

sub _ping{
  my $self = shift;
  return gettimeofday();
}

#--- calllback function to process results when building our hostkey list
sub _hostkey_cb{
    my $self      = shift;
    my $key       = shift;
    my $ref       = shift;
    my $reply     = shift; 

    return if(!defined($reply));
    push(@$ref, $key . "," . $reply);
}


#--- returns a hash that maps ip to the host key
sub _gen_hostkeys{
  my $self        = shift;
  my $ipaddrs     = shift;
  my $index       = shift;
  my $redis       = $self->redis;

  my $results_back = 

  my @results;
  my $cursor = 0;

  try {
      
      #--- the timestamps are kept in a different db "1" vs "0"
      $redis->select(1);
      
      my $keys;
      foreach my $ip (@$ipaddrs){
          while(1){
              my $ip_str = $ip . ",*";
              #---- get the set of hash entries that match our pattern
              ($cursor,$keys) =  $redis->scan($cursor,MATCH=>$ip_str,COUNT=>200);
              foreach my $key (@$keys){
                  #--- iterate on the returned OIDs and pull the values associated to each host
                  $redis->lindex($key, $index, sub { $self->_hostkey_cb($key,\@results,@_)});
              }
              last if($cursor == 0);
          }
          
          #--- wait for all the hgetall responses to return
          $redis->wait_all_responses;
          
      }
  } catch {
      $self->logger->error(" in gen_hostkeys: ". $_);
  };
  

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

    foreach my $hk (@$hkeys){
        #--- Host Ip, Collector ID, TimeStamp
        my ($ip,$id,$oid,$ts) = split(/,/,$hk);
        my $val = shift @$reply;
        next if(!defined $val);                  #-- this OID key has no relevance to the IP in question if null here
        $ref->{$ip}{$key}{'value'} = $val;       #-- external data representation is inverted from what we store
        $ref->{$ip}{$key}{'time'} = $ts;
    }
}


sub _get{
  #--- implementation using pipelining and callbacks 
  my $self      = shift;
  my $pollcycle = shift;
  my $params    = shift;
  my $ipaddrs   = $params->{'ipaddrs'}{'value'};
  my $oidmatch  = $params->{'oidmatch'}{'value'};
  my $redis     = $self->redis;

  my %results;

  try {
    #--- convert the set of interesting ip address to the set of internal keys used to retrive data 
    my $hkeys = $self->_gen_hostkeys($ipaddrs,$pollcycle);

    foreach my $oid (@$oidmatch){
      my $cursor = 0;
      my $keys;
      while(1){
        #---- get the set of hash entries that match our pattern
        ($cursor,$keys) =  $redis->scan($cursor,MATCH=>$oid,COUNT=>200);
        foreach my $key (@$keys){
          #--- iterate on the returned OIDs and pull the values associated to each host
          my $vals =$redis->hmget($key,@$hkeys,sub {$self->_get_cb($hkeys,$key,\%results,@_);});
        } 
        last if($cursor == 0);
      }
    }

    #--- wait for all pending responses to hmget requests
    $redis->wait_all_responses;
    
  } catch {
    $self->logger->error(" in get: ". $_);
  };

  return \%results;
}
sub _rate{
  my ($self,$cur_val,$cur_ts,$pre_val,$pre_ts,$context) = @_;
  my $et      = $cur_ts - $pre_ts;
  my $delta   = $cur_val - $pre_val;
  my $max_val = 2**32;
  
  if($et <= 0){
    #--- not elapse time cant calcualte rate / 0 and all
    return 0;
  }

  if($cur_val > $max_val || $pre_val > $max_val){
    #--- we have a value greater than 32bit assume 64bit counter
    $max_val = 2**64;
  }

  if($delta < 0){
    #--- counter wrap
    if(defined $context){
      $self->logger->debug("  counter wrap: $context");
    }
    $delta = ($max_val - $pre_val) + $cur_val;
  }  

  return $delta / $et;
}


sub _get_rate{
  my $self       = shift;
  my $params     = shift;
  my $redis      = $self->redis;

  my %current_data;
  my %previous_data;

  #--- get the data for the current and a past poll cycle
  my $current_data  = $self->_get(0,$params);
  my $previous_data = $self->_get(2,$params);

  my %results;
    
  #iterate over current and previous data to calculate rates where sensible
  foreach my $ip (keys (%$current_data)){
    foreach my $oid (keys (%{$current_data->{$ip}})){
      my $current_val  = $current_data->{$ip}{$oid}{'value'};
      my $current_ts   = $current_data->{$ip}{$oid}{'time'};
      my $previous_val = $previous_data->{$ip}{$oid}{'value'};
      my $previous_ts  = $previous_data->{$ip}{$oid}{'time'};

      #--- sanity check
      if(!defined $current_val || !defined $previous_val || 
        !defined $current_ts  || !defined $previous_ts){
        #--- incomplete data
        next;
      } 

      if(!($current_val =~/\d+$/)){
        #--- not a number
        next;
      }

      $results{$ip}{$oid} = $self->_rate($current_val,$current_ts,
					 $previous_val,$previous_ts,
					 "$ip->$oid");
    }
  } 
  return \%results;
}


1;
