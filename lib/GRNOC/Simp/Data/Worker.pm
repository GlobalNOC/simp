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
    $self->logger->debug( $self->worker_id." starting." );
    $self->_start();
    sleep 2;
  }

}

sub stop{
  my ($self ) = @_;
  exit;
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

    $SIG{'INT'} = sub {
      $self->logger->info( "Received SIG INT." );
      $self->stop();
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
                                read_timeout => 2,
                                write_timeout => .3,
                        );

    $self->_set_redis( $redis );

    $self->logger->debug( 'Setup RabbitMQ' );

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new( 	queue_name => "Simp.Data",
							topic => "Simp.Data",
							exchange => "Simp",
							user => $rabbit_user,
							pass => $rabbit_pass,
							host => $rabbit_host,
							port => $rabbit_port);


    my $method = GRNOC::RabbitMQ::Method->new(	name => "get",
						callback =>  sub {
								   my ($method_ref,$params) = @_; 
								   my $time = time();
								   if(defined($params->{'time'}{'value'})){
								       $time = $params->{'time'}{'value'};
								   }
								   return $self->_get($time,$params); 
								 },
						description => "function to pull SNMP data out of cache");

    $method->add_input_parameter( name => "oidmatch",
                                  description => "redis pattern for specifying the OIDS of interest",
                                  required => 1,
                                  multiple => 1,
                                  pattern => $GRNOC::WebService::Regex::TEXT);


    $method->add_input_parameter( name => "node",
                                  description => "array of ip addresses or node_names to fetch data for",
                                  required => 1,
                                  schema => { 'type'  => 'array',
                                              'items' => [ 'type' => 'string',
                                                         ]
                                            } );

    $method->add_input_parameter( name => "time",
                                  description => "unix timestamp of data you are interested in",
                                  required => 0,
                                  schema => { 'type'  => 'integer'  } );

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
    
    
    $method->add_input_parameter( name => "period",
				  description => "time period (in seconds) for the rate, basically this is (now - period)",
				  required => 0,
				  multiple => 0,
				  default => 60,
				  pattern => $GRNOC::WebService::Regex::ANY_NUMBER);
    
    $method->add_input_parameter( name => "node",
                                  description => "array of ip addresses / node names to fetch data for",
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

    #--- build host key cache every second 
#    $self->_gen_hostkeys();
#
#    $self->{'host_key_timer'} = AnyEvent->timer( after => 1, 
#						 interval => 10, 
#						 cb => sub {
#						     $self->_gen_hostkeys(); 
#						 });
#    
#    sleep(1);
    
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

#--- callback to handle results from the hmgets issued in _get
sub _get_cb{
    my $self      = shift;

    my $key       = shift;
    my $ref       = shift;
    my $reply     = shift;
    my $error     = shift;

    my ($node, $oid, $poller, $time) = split(',',$key);

    $ref->{$node}{$oid}{'value'} = $reply;
    $ref->{$node}{$oid}{'time'}  = $time;

}

sub _find_host_key_time{
    my $self = shift;
    my %params = @_;

    my $host = $params{'host'};
    my $oid = $params{'oid'};
    my $requested = $params{'requested'};

    $self->redis->select(1);
    
    my $match = "$host,*";

    my $keys;
    my $cursor = 0;
    my @temp_times;
    while(1){
	#--- get the set of hash entries that match our pattern
	($cursor,$keys) =  $self->redis->scan($cursor,MATCH=>$match,COUNT=>2000);
	foreach my $key (@$keys){
	    my @vals = split(',',$key);
	    if($oid =~ /$vals[1]/){
		#get all the values in the array so we can determine which one we want
                push(@temp_times, {time => $vals[3], collector => $vals[2]});
	    }else{
		next;
	    }
	}
        last if($cursor == 0);
    }

    my $final_key;
    #ok found all the possible keys

    #sort the times
    my @sorted_times = reverse sort { $a->{'time'} <=> $b->{'time'} } @temp_times;

    foreach my $obj (@sorted_times){
        $self->logger->debug("TIME: " . $obj->{'time'} . " vs requested " . $requested);
        #find the time closest to the one I want!
        if($obj->{'time'} < $requested && !defined($final_key)){
            $final_key = $host . "," . $oid . "," . $obj->{'collector'} . "," . $obj->{'time'};
        }
    }

    $self->redis->select(0);
    return $final_key;
}


sub _get{
  #--- implementation using pipelining and callbacks 
    my $self      = shift;
  my $requested = shift;
  my $params    = shift;
  my $node      = $params->{'node'}{'value'};
  my $oidmatch  = $params->{'oidmatch'}{'value'};
  my $redis     = $self->redis;

  my %results;

  $self->logger->debug("processing get request for time " . $requested);

  try {
    #--- convert the set of interesting ip address to the set of internal keys used to retrive data 

    foreach my $oid (@$oidmatch){
	foreach my $host (@$node){

	    #find the correct key to fetch for!

	    my $match = $self->_find_host_key_time( host => $host,
						    oid => $oid, 
						    requested => $requested);
	    
	    if(!defined($match)){
		next;
	    }
	    
	    if(!($oid =~ /\*/)){

		$redis->get($match, sub { $self->_get_cb($match, \%results, @_) });
		
	    }else{
                my $keys;
		my $cursor = 0;

                $self->logger->debug("Scanning for " . $match);

		while(1){
		    #--- get the set of hash entries that match our pattern
		    ($cursor,$keys) =  $redis->scan($cursor,MATCH=>$match,COUNT=>2000);
		    foreach my $key (@$keys){
			#--- iterate on the returned OIDs and pull the values associated to each host
			$redis->get($key, sub { $self->_get_cb($key, \%results, @_); });
		    }
				
                    last if($cursor == 0);
                }
	    }
	}
    }
    
    $self->logger->debug("Waiting for responses");

    #--- wait for all pending responses to hmget requests
    $redis->wait_all_responses;
    
  } catch {
      $self->logger->error(" in get: ". $_);
  };
  
  $self->logger->debug("Response ready!");

  return \%results;
}
sub _rate{
  my ($self,$cur_val,$cur_ts,$pre_val,$pre_ts,$context) = @_;
  my $et      = $cur_ts - $pre_ts;
  if(!($cur_val =~ /\d+$/)){
      #not a number... 
      return $cur_val;
  }
  
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

  if(!defined($params->{'period'}{'value'})){
      $params->{'period'}{'value'} = 60;
  }

  #--- get the data for the current and a past poll cycle
  my $current_data  = $self->_get(time(),$params);
  my $previous_data;

  my @ips = keys %{$current_data};
  if(defined($ips[0])){
      my @oids = keys %{$current_data->{$ips[0]}};

      if(defined($oids[0])){

          my $time = $current_data->{$ips[0]}{$oids[0]}{'time'};
          $time -= $params->{'period'}{'value'};

          $previous_data = $self->_get($time,$params);
      }
  }

  my %results;

  return \%results if !defined($previous_data);
    
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

      if(!($current_val =~/^\d+$/)){
        #--- not a number
        next;
      }

      $results{$ip}{$oid}{'value'} = $self->_rate($current_val,$current_ts,
						  $previous_val,$previous_ts,
						  "$ip->$oid");
      $results{$ip}{$oid}{'time'}  = $current_ts;

    }
  } 
  return \%results;
}


1;
