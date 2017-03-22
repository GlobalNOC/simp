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
use POSIX;
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

sub _find_group{
    my $self = shift;
    my %params = @_;

    my $host = $params{'host'};
    my $oid = $params{'oid'};
    my $requested = $params{'requested'};

    my @host_groups;

    try{
	$self->redis->select(3);
	@host_groups = $self->redis->smembers($host);
	$self->redis->select(0);
    }catch{
	$self->redis->select(0);
	$self->logger->error("Error fetching all groups host is a part of");
	return;
    };	

    my @new_host_groups;
    foreach my $hg (@host_groups){
	my ($base_oid, $group, $interval) = split(',', $hg);
	if($oid =~ /$base_oid/){
	    push(@new_host_groups, { base_oid => $base_oid, group => $group, interval => $interval});
	}
    }
    
    #ok we are close! we only have matching host groups...
    #if we have none... return undef
    if(scalar(@new_host_groups) <= 0){
	$self->logger->error("NO host groups found for $host and $oid"); 
	return;
    }

    #if we have 1 return that group
    if(scalar(@new_host_groups) == 1){
	return $new_host_groups[0];
    }

    #ok if we have multiple... sort by length of base_oid
    my @sorted = sort { length($a->{'base_oid'}) <=> length($b->{'base_oid'}) } @new_host_groups;

    my @longest_base_oids;
    push(@longest_base_oids, $sorted[0]);

    for(my $i=1; $i<=$#sorted; $i++){
	if(length($sorted[$i-1]->{'base_oid'}) == length($sorted[$i]->{'base_oid'})){
	    push(@longest_base_oids, $sorted[$i]);
	}else{
	    last;
	}
    }

    if(scalar(@sorted) == 1){
	return $sorted[0];
    }
    
    #damn ok so we have 2 that are the same length now check the intervals...
    my @sorted_intervals = sort { $a->{'interval'} <=> $b->{'interval'} } @sorted;

    return $sorted[0];

}

sub _find_key{

    my $self = shift;
    my %params = @_;

    my $host = $params{'host'};
    my $oid = $params{'oid'};
    my $requested = $params{'requested'};

    my $group = $self->_find_group( host => $host, 
				    oid => $oid,
				    requested => $requested);
    
    if(!defined($group)){
	$self->logger->error("unable to find group for $host $oid");
	return;
    }

    #ok so we have the group name we want now... grab the key that leads to the current time chunk
    $self->redis->select(2);
    my $res = $self->redis->get($host . "," . $group->{'group'});
    $self->redis->select(0);
    
    #key should be poll_id,ts
    my ($poll_id,$time) = split(',',$res);

    #if the requested time is newer than our poll time OR the poll time - the requested time is less than 1 poll interval return the current one
    my $lookup;
    if($requested > $time){
	$lookup = $host . "," . $group->{'group'} . "," . $poll_id;
    }else{
	#find the closest poll cycle
	my $poll_ids_back = floor(($time - $requested) / $group->{'interval'});
	if($poll_id >= $poll_ids_back){
	    $self->logger->error("looking " . $poll_ids_back);
	    $lookup = $host . "," . $group->{'group'} . "," . ($poll_id - $poll_ids_back);
	    $self->logger->error("lookup key: " . $lookup);
	}else{
	    $self->logger->error("No time available that matches the requested time!");
	    return;
	}
    }


    #ok so lookup is now defined
    #lookup will give us the right set to sscan through
    $self->redis->select(1);
    my $set = $self->redis->get($lookup);
    $self->redis->select(0);

    return $set;
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
	my $scan_start;
	foreach my $oid (@$oidmatch){
	    foreach my $host (@$node){
		
		#find the correct key to fetch for!
		my $set = $self->_find_key( host => $host,
					    oid => $oid,
					    requested => $requested);

		my ($host, $group, $time) = split(',',$set);
		if(!defined($set)){
		    $self->logger->error("Unable to find set to look at");
		    next;
		}
		

		my $keys;
		my $cursor = 0;
		while(1){
		    #--- get the set of hash entries that match our pattern
		    ($cursor,$keys) =  $redis->sscan($set, $cursor,MATCH=>$oid . "*",COUNT=>2000);
		    foreach my $key (@$keys){
			
			$key =~ /([\d+|\.]+),(.*)/;
			my $oid = $1;
			my $value = $2;

			$results{$host}{$oid}{'value'} = $value;
			$results{$host}{$oid}{'time'} = $time;
		    }
		    
		    last if($cursor == 0);
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
