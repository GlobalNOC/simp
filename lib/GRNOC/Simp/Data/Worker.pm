package GRNOC::Simp::Data::Worker;

use strict;
use Carp;
use List::MoreUtils qw(none);
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
=head2 public attribures

=over 12

=item config

=item logger

=item worker_id

=back

=cut

has config => ( is => 'ro',
                required => 1 );

has logger => ( is => 'ro',
                required => 1 );

has worker_id => ( is => 'ro',
               required => 1 );


### internal attributes ###
=head2 private attributes

=over 12

=item is_running

=item redis

=item dispatcher

=item need_restart

=back

=cut
has is_running => ( is => 'rwp',
                    default => 0 );

has redis => ( is => 'rwp' );

has dispatcher  => ( is => 'rwp' );

has need_restart => (is => 'rwp',
                    default => 0 );


=head2 start

=cut


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

=head2 stop

=cut

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

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new(  queue_name => "Simp.Data",
                                                        topic => "Simp.Data",
                                                        exchange => "Simp",
                                                        user => $rabbit_user,
                                                        pass => $rabbit_pass,
                                                        host => $rabbit_host,
                                                        port => $rabbit_port);


    my $method = GRNOC::RabbitMQ::Method->new(  name => "get",
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

# Is the first argument, interpreted as an OID, a prefix of
# the second argument, also interpreted as an OID?
sub _is_oid_prefix{
    my $a = shift;
    my $b = shift;

    return 0 if length($a) > length($b);

    my $len = length($a);
    my $next_char = substr($b, $len, 1);

    return ((substr($b, 0, $len) eq $a) &&
            ($next_char eq '.' || $next_char eq ''));
}

sub _find_groups{
    my $self = shift;
    my %params = @_;

    my $host = $params{'host'};
    my $oid = $params{'oid'};
    my $requested = $params{'requested'};

    my %host_groups;

    # Remove any ".*" suffixes
    $oid =~ s/(\.\*+)*$//;

    try{
        $self->redis->select(3);
        %host_groups = $self->redis->hgetall($host);
        $self->redis->select(0);
    }catch{
        $self->redis->select(0);
        $self->logger->error("Error fetching all groups host is a part of");
        return;
    };

    my @new_host_groups;
    foreach my $base_oid (keys %host_groups){
        my ($group, $interval) = split(',', $host_groups{$base_oid});

        # group is a candidate if its base OID is a prefix of ours
        if(_is_oid_prefix($base_oid, $oid)){
            push(@new_host_groups, { base_oid => $base_oid, group => $group, interval => $interval});
        }
    }

    # If we have 1 candidate, return that group
    if(scalar(@new_host_groups) == 1){
        return [ $new_host_groups[0] ];
    }elsif(scalar(@new_host_groups) > 1){

        # If we have multiple matching candidates, sort lexicographically by
        # (length of base_oid descending, interval ascending)
        my @sorted = sort { -(length($a->{'base_oid'}) <=> length($b->{'base_oid'})) or
                            ($a->{'interval'} <=> $b->{'interval'}) } @new_host_groups;

        # We now have in $sorted[0] the most specific match; if there are multiple
        # most-specific matches, $sorted[0] is the one with the shortest interval.
        return [ $sorted[0] ];
    }

    # If we got here, we had no candidates - groups whose base OID is a prefix
    # of $oid, the base OID of the tree we're interested in. That said, all hope
    # is not lost yet - it may be that there are one or more groups whose
    # base OID has $oid as a prefix! We now look for these. If we find more than
    # one, we then weed out groups that are redundant, i.e., groups for which
    # there is a "wider" (shorter-prefix) group with the same or shorter interval.

    @new_host_groups = ();
    foreach my $base_oid (keys %host_groups){
        my ($group, $interval) = split(',', $host_groups{$base_oid});

        # This time around, group is a candidate if our OID is a prefix of its base
        if(_is_oid_prefix($oid, $base_oid)){
            push(@new_host_groups, { base_oid => $base_oid, group => $group, interval => $interval});
        }
    }

    # Sort the groups lexicographically by
    # (length of base_oid ascending, interval ascending)
    my @sorted = sort { (length($a->{'base_oid'}) <=> length($b->{'base_oid'})) or
                        ($a->{'interval'} <=> $b->{'interval'}) } @new_host_groups;

    # Filter out redundant groups
    my @final_list = ();
    foreach my $group (@sorted){
        if(none { _is_oid_prefix($_->{'base_oid'}, $group->{'base_oid'}) &&
                  $_->{'interval'} <= $group->{'interval'} } @final_list){
            push @final_list, $group;
        }
    }
    # For the purposes of _find_groups's callees, we want to work from wider to
    # narrower groups; @final_list has the groups in the right order.

    if(scalar(@final_list) <= 0){
        $self->logger->error("NO host groups found for $host and $oid");
    }

    return \@final_list;
}

sub _find_keys{

    my $self = shift;
    my %params = @_;

    my $host = $params{'host'};
    my $oid = $params{'oid'};
    my $requested = $params{'requested'};

    my $groups = $self->_find_groups( host => $host,
                                      oid => $oid,
                                      requested => $requested);

    if(!defined($groups) || (scalar(@$groups) <= 0) || (none { defined($_) } @$groups)){
        $self->logger->error("unable to find group for $host $oid");
        return;
    }

    my @sets;

    foreach my $group (@$groups){
        next if !defined($group);
        #ok so we have the group name we want now... grab the key that leads to the current time chunk
        $self->redis->select(2);
        my $res = $self->redis->get($host . "," . $group->{'group'});
        $self->redis->select(0);
        next if !defined($res);

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
                $self->logger->debug("looking " . $poll_ids_back);
                $lookup = $host . "," . $group->{'group'} . "," . ($poll_id - ($poll_ids_back +1));
                $self->logger->debug("lookup key: " . $lookup);
            }else{
                $self->logger->debug("No time available that matches the requested time!");
                return;
            }
        }


        #ok so lookup is now defined
        #lookup will give us the right set to sscan through
        $self->redis->select(1);
        my $set = $self->redis->get($lookup);
        push @sets, $set if defined($set);
        $self->redis->select(0);
    }

    return \@sets;
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

                #find the correct keys to fetch for!
                my $sets = $self->_find_keys( host => $host,
                                              oid => $oid,
                                              requested => $requested);

                if(!defined($sets) || (scalar(@$sets) <= 0) || (none { defined($_) } @$sets)){
                    $self->logger->error("Unable to find set to look at");
                    next;
                }

                foreach my $set (@$sets){
                    next if !defined($set);

                    my ($host, $group, $time) = split(',',$set);

                    my $keys;
                    my $cursor = 0;
                    while(1){
                        #--- get the set of hash entries that match our pattern
                        ($cursor,$keys) =  $redis->sscan($set, $cursor,MATCH=>$oid . "*",COUNT=>2000);
                        foreach my $key (@$keys){

                            $key =~ /^([^,]*),(.*)/;
                            my $oid = $1;
                            my $value = $2;

                            $results{$host}{$oid}{'value'} = $value;
                            $results{$host}{$oid}{'time'} = $time;
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
