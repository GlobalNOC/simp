package GRNOC::Simp::Data::Worker;

use strict;
use warnings;

#use Data::Dumper;

use Carp;
use List::MoreUtils qw(none);
use Moo;
use POSIX;
use Redis::Fast;
use Scalar::Util qw(looks_like_number);
use Time::HiRes qw(gettimeofday tv_interval);
use Syntax::Keyword::Try;

use GRNOC::RabbitMQ::Method;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::WebService::Regex;

# Define a maximum value for uint32 and uint64
# These are used during rate calculations
use constant {
    MAX_UINT32 => 2**32,
    MAX_UINT64 => 2**64
};

=head2 public attribures
=over 12

=item config
=item logger
=item worker_id

=back
=cut
has config => (
    is       => 'ro',
    required => 1
);
has logger => (
    is       => 'ro',
    required => 1
);
has worker_id => (
    is       => 'ro',
    required => 1
);

=head2 private attributes
=over 12

=item is_running
=item redis
=item dispatcher

=back
=cut
has redis => (is => 'rwp');
has dispatcher => (is => 'rwp');
has is_running => (
    is      => 'rwp',
    default => 0
);


=head2 start
    Starts the Data.Worker.
    If the worker can't connect to RabbitMQ, it will retry every 2s.
=cut
sub start {
    my ($self) = @_;

    # A successful call to _start() will stop this loop from re-running.
    # The loop is locked while the RabbitMQ Dispatcher listens for requests.
    # In any other case, this loop will run every 2s retrying the RabbitMQ Dispatcher connection.
    # If the dispatcher/RMQ dies while the worker is running, the loop will continue again.
    # The loop only exits when the worker is terminated by a signal handler.
    while (1) {
        $self->logger->debug($self->worker_id . " starting.");
        $self->_start();
        sleep 2;
    }

}


=head2 stop
    Stops the Data.Worker by exiting
=cut
sub stop {
    my ($self) = @_;
    exit;
}


=head2 _start()
    Private start method called by start() to start the worker program
    This will connect the worker to Redis and RabbitMQ
=cut
sub _start {
    my ($self) = @_;

    my $worker_id = $self->worker_id;

    # flag that we're running
    $self->_set_is_running(1);

    #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # BEHOLD! TIS' THE FORSAKEN WORKER NAME FORMAT!
    #XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    # ALL WHO CHANGE IT WILL BE CURSED BY REDIS...
    # TO DEBUG FOR ALL ETERNITY!
    $0 = "simp_data ($worker_id) [worker]";

    # Set the signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info("Received SIG TERM.");
        $self->stop();
    };
    $SIG{'HUP'} = sub {
        $self->logger->info("Received SIG HUP.");
    };

    $SIG{'INT'} = sub {
        $self->logger->info("Received SIG INT.");
        $self->stop();
    };

    # Get RabbitMQ configuration variables
    my $rabbit_host = $self->config->get('/config/rabbitmq/@ip');
    my $rabbit_port = $self->config->get('/config/rabbitmq/@port');
    my $rabbit_user = $self->config->get('/config/rabbitmq/@user');
    my $rabbit_pass = $self->config->get('/config/rabbitmq/@password');

    my $redis = $self->_connect_to_redis();

    $self->_set_redis($redis);

    # Connect the RabbitMQ Dispatcher and add Simp.Data methods to it
    $self->logger->debug('Setting up RabbitMQ');
    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new(
        queue_name => "Simp.Data",
        topic      => "Simp.Data",
        exchange   => "Simp",
        user       => $rabbit_user,
        pass       => $rabbit_pass,
        host       => $rabbit_host,
        port       => $rabbit_port
    );

    # Create the main get() method for the worker
    # The method pulls requested SNMP data from Redis if it doesn't need rate calculations
    my $method = GRNOC::RabbitMQ::Method->new(
        name        => "get",
        description => "Pulls SNMP data from Redis",
        callback => sub {
            my ($method_ref, $params) = @_;

            # Use the current time unless time was given as a parameter
            my $time = defined($params->{time}{value}) ? $params->{time}{value} : time();
            
            # Return the result of calling the Worker's _get() method using request parameters
            return $self->_get($time, $params);
        }
    );
    # Add "oidmatch", "node", and "time" as parameters for the Dispatcher's "get" method
    $method->add_input_parameter(
        name        => "oidmatch",
        description => "Pattern for specifying the OIDs wanted from Redis",
        required    => 1,
        multiple    => 1,
        pattern     => $GRNOC::WebService::Regex::TEXT
    );
    $method->add_input_parameter(
        name        => "node",
        description => "Array of IPs or hostnames needing data from Redis",
        required    => 1,
        schema      => {
            'type'  => 'array',
            'items' => ['type' => 'string',]
        }
    );
    $method->add_input_parameter(
        name        => "time",
        description => "Unix timestamp for the data wanted from Redis",
        required    => 0,
        schema      => {'type' => 'integer'}
    );
    # Register the main "get" method for the Dispatcher
    $dispatcher->register_method($method);


    # Create the "get_rate" method for the Dispatcher
    $method = GRNOC::RabbitMQ::Method->new(
        name        => "get_rate",
        description => "Pulls SNMP data from Redis and calculates values as rates using an interval",
        callback    => sub {
            my ($method_ref, $params) = @_;
            # Return the result of calling the Worker's _get_rate() method using request parameters
            return $self->_get_rate($params);
        }
    );
    # Add "oidmatch", "period", and "node" as parameters for the Dispatcher's "get_rate" method
    $method->add_input_parameter(
        name        => "oidmatch",
        description => "redis pattern for specifying the OIDS of interest",
        required    => 1,
        multiple    => 1,
        pattern     => $GRNOC::WebService::Regex::TEXT
    );
    $method->add_input_parameter(
        name        => "period",
        description => "time period (in seconds) for the rate, basically this is (now - period)",
        required    => 0,
        multiple    => 0,
        default     => 60,
        pattern     => $GRNOC::WebService::Regex::ANY_NUMBER
    );
    $method->add_input_parameter(
        name        => "node",
        description => "array of ip addresses / node names to fetch data for",
        required    => 1,
        schema      => {
            'type'  => 'array',
            'items' => ['type' => 'string',]
        }
    );
    # Register the "get_rate" method for the Dispatcher
    $dispatcher->register_method($method);

    # Create and register the "ping" method for the Dispatcher
    $method = GRNOC::RabbitMQ::Method->new(
        name        => "ping",
        callback    => sub {$self->_ping($dispatcher)},
        description => "Method to test RabbitMQ latency"
    );
    $dispatcher->register_method($method);

    # Start the RabbitMQ Dispatcher
    # Once started, the Dispatcher will listen for method requests from RabbitMQ
    if ($dispatcher && $dispatcher->connected) {
        $self->logger->debug('Entering RabbitMQ event loop');
        $dispatcher->start_consuming();
    }
    else {
        $self->logger->error('ERROR: Simp.Data could not connect the RabbitMQ dispatcher!');
    }

    # Return when a handler calls stop_consuming after issues reaching Redis require a re-init
    return;
}


=head2 _ping()
    Called as the callback for the Dispatcher's "ping" method.
    Simply returns the time of day.
=cut
sub _ping {
    my $self = shift;
    return gettimeofday();
}


=head2 _is_oid_prefix()
    Returns true if the first OID is a prefix of the second OID.
=cut
sub _is_oid_prefix {
    my ($a, $b) = @_;

    return 0 if length($a) > length($b);

    my $len       = length($a);
    my $next_char = substr($b, $len, 1);

    return ((substr($b, 0, $len) eq $a) && ($next_char eq '.' || $next_char eq ''));
}


=head2 _best_host_groups
    Returns the most relevant host group(s), wrapped in an array reference, out of an array of host groups.
    This is used by _find_groups() to determine which host group is most relevant.
    The array is first sorted by length of base_oid in descending order, then by interval in ascending order.
    When reverse is true, length of base_oid is sorted in ascending order instead.
    The resulting element at index 0 is the best OID match or best match with the shortest interval.
=cut
sub _best_host_groups {
    my $self    = shift;
    my $groups  = shift;
    my $reverse = shift;

    # If there's only one group, return it wrapped in an array ref
    if (scalar(@$groups) == 1) {
        return [$groups->[0]]
    }

    # An array for the groups sorted by relevance
    my @sorted;

    # Sorts with base OID in descending order
    unless ($reverse) {
        @sorted = sort {-(length($a->{'base_oid'}) <=> length($b->{'base_oid'})) or ($a->{'interval'} <=> $b->{'interval'})} @$groups;
        return [$sorted[0]];
    }
    # Sorts with base OID in ascending order
    else {
        @sorted = sort {(length($a->{'base_oid'}) <=> length($b->{'base_oid'})) or ($a->{'interval'} <=> $b->{'interval'})} @$groups;
        
        # Array with redundant groups removed from @sorted
        my @filtered = ();
        for my $group (@sorted) {
            if (none {_is_oid_prefix($_->{'base_oid'}, $group->{'base_oid'}) && $_->{'interval'} <= $group->{'interval'};} @filtered) {
                push(@filtered, $group);
            }
        }
        return \@filtered;
    }
}


=head2 _find_groups()
    Finds data collection groups for a host
=cut
sub _find_groups {
    my $self      = shift;
    my %params    = @_;

    # Get the hostname, requested OID, and Redis session
    my $host      = $params{'host'};
    my $oid       = $params{'oid'};
    my $redis     = $self->redis;

    # Remove any ".*" suffixes from the requested OID
    $oid =~ s/(\.\*+)*$//;

    # Get group name mappings for OIDs associated with the host
    my %host_oid_map;
    try {
        $redis->select(3);

        # Scan for keys using the hostname or IP
        # The returned keys are in the format "host:port" or "host:port:context"
        # We then use the keys to get all OID => "group,interval" hashes associated with them
        my ($cursor, $keys) = (0, undef);
        while (1) {
            ($cursor, $keys) = $redis->scan(
                $cursor,
                MATCH => sprintf("%s:*", $host),
                COUNT => 2000
            );

            for my $key (@$keys) {
                # Redis returns a hash as an array ref
                # We convert the array ref to a hash when adding it to our own hash under the host/ip key
                %{$host_oid_map{$key}} = $redis->hgetall($key);
            }

            # Redis has indicated that it finished all iterations
            last if ($cursor == 0);
        }
    }
    catch ($e) {
        $self->logger->error("[_find_groups] Error fetching polling groups with data for $host: $e");
        return;
    };


    # Process the host groups from what Redis returned
    # We will return a hashref of host keys pointing to their array of groups
    my $groups = {};
    while (my ($host_key, $oid_group_map) = each(%host_oid_map)) {

        my @host_groups;
        my @host_groups_rev;

        while (my ($base_oid, $group_key) = each(%$oid_group_map)) {

            # Parse the group name and interval from the value string for the OID
            my ($group, $interval) = split(',', $group_key);

            # Group match if the base OID is a prefix of the requested OID
            if (_is_oid_prefix($base_oid, $oid)) {
                push(@host_groups, {base_oid => $base_oid, name => $group, interval => $interval});
            }
            # Group match if the requested OID is a prefix of the base_oid for reverse lookups
            elsif (_is_oid_prefix($oid, $base_oid)) {
                push(@host_groups_rev, {base_oid => $base_oid, name => $group, interval => $interval});
            }
        }

        # Set the best matching host group if any were found
        if (scalar(@host_groups) >= 1) {
            $groups->{$host_key} = $self->_best_host_groups(\@host_groups);
        }
        # Set the best matches from a reverse lookup
        else {
            # If we got here, we had no groups where $base_oid was a prefix of the OID tree we're interested in.
            # This might be because there are one or more groups whose base OID has $oid as a prefix!
            # We now look for these. If we find more than one, we then remove groups that are redundant,
            # i.e., groups for which there is a shorter-prefix group with the same or shorter interval.
            $groups->{$host_key} = $self->_best_host_groups(\@host_groups_rev, 1);
        }

        # Log an error if no groups were defined
        $self->logger->error("[_find_groups] No host groups found for $host and $oid") unless ($groups->{$host_key});
    }

    return $groups;
}

=head2 _connect_to_redis()
     Helper function that helps connecting to the redis server 
     and returns the redis object reference
=cut
sub _connect_to_redis {
    my ($self) = @_;

    # Get Redis configuration variables
    my $redis_host            = $self->config->get('/config/redis/@ip');
    my $redis_port            = $self->config->get('/config/redis/@port');
    my $redis_reconnect       = $self->config->get('/config/redis/@reconnect');
    my $redis_reconnect_every = $self->config->get('/config/redis/@reconnect_every');
    my $redis_read_timeout    = $self->config->get('/config/redis/@read_timeout');
    my $redis_write_timeout   = $self->config->get('/config/redis/@write_timeout');

    # Connect the worker to Redis and store the session
    $self->logger->debug(sprintf(
        "Connecting to Redis %s:%s => {reconnect: %s, every: %s, read_timeout: %s, write_timeout: %s}",
        $redis_host,
        $redis_port,
        $redis_reconnect,
        $redis_reconnect_every,
        $redis_read_timeout,
        $redis_write_timeout
    ));

    my $redis;
    my $redis_connected = 0;

    # Try reconnecting to redis. Only continue when redis is connected. 
    while (!$redis_connected) {
        try {
            $redis = Redis::Fast->new(
                server        => sprintf("%s:%s", $redis_host, $redis_port),
                reconnect     => $redis_reconnect,
                every         => $redis_reconnect_every,
                read_timeout  => $redis_read_timeout,
                write_timeout => $redis_write_timeout
            );
            $redis_connected = 1;
        }
        catch($e) {
            $self->logger->error(sprintf("Error connecting to Redis: %s. Trying Again...", $e));
        }
    }

    return $redis;
}

=head2 _find_keys()
    Retrieves all of the relevant keys to SNMP Polling data.
    It does this by first finding the most relevant host groups for the request in Redis.
    Then, 
=cut
sub _find_keys {
    my $self   = shift;
    my %params = @_;

    my $host      = $params{'host'};
    my $oid       = $params{'oid'};
    my $requested = $params{'requested'};
    my $redis     = $self->_connect_to_redis();

    # Get the host's collection groups and their keys from Redis
    my $host_groups = $self->_find_groups(
        host      => $host,
        oid       => $oid,
    );

    # Ensure that groups were found for the host and OID
    if (!defined($host_groups) || (none {defined($host_groups->{$_})} keys(%$host_groups))) {
        $self->logger->error("[_find_keys] Unable to find groups for $host -> $oid");
        return;
    }
   
    # Select the hostgroup DB in Redis
    $redis->select(2, sub{});

    # Hash of response objects containing hostgroup keys
    my $hostgroup_keys = {};

    # Array of keys for the data to scan through for the host+group
    my $keys = [];

    while (my ($host_key, $groups) = each(%$host_groups)) {

        # Try to find keys by iterating over the array of possible groups from _find_groups()
        for my $group (@$groups) {

            # Skip undefined groups
            next if !defined($group);

            # Get the Poll ID & Timestamp for the hostgroup combination asyncronously
            try {
                $redis->get(
                    sprintf("%s,%s", $host_key, $group->{name}),
                    sub {
                        my $group_data = shift;
                        return sub {
                            my ($reply, $err) = @_;
                            $hostgroup_keys->{$host_key} = {'result' => $reply, 'group' => $group_data} if ($reply);
                        };
                    }->($group)
                );
            }
            catch ($e) {
                $self->logger->error("[_find_keys] Could not retrieve Poll ID/Timestamp data for $host -> $group->{group}: $e");
                next;
            };
        }
    }

    # Wait until all the group keys have been returned
    $redis->wait_all_responses();

    # Select the keys DB in Redis 
    $redis->select(1, sub{});

    # Lookup the returned hostgroup keys from earlier in the keys DB of redis
    while (my ($host_key, $group_data) =  each(%$hostgroup_keys)) {

        # Pull the hostgroup key and group name from the reply
        my $res   = $group_data->{result};
        my $group = $group_data->{group};
    
        # Get the Poll ID and Timestamp from the returned value for the group
        my ($poll_id, $time) = split(',', $res);

        # Set a lookup key if our Poll ID is valid for the current time
        my $lookup;
        if ($requested > $time) {
            $lookup = sprintf("%s,%s,%s", $host_key, $group->{name}, $poll_id);
        }
        else {

            # Determine the most recent poll cycle
            my $poll_ids_back = floor(($time - $requested) / $group->{'interval'});

            # Set the lookup if the retrieved Poll ID is the same or newer than the closest poll cycle 
            if ($poll_id >= $poll_ids_back) {
                $lookup = sprintf("%s,%s,%s", $host_key, $group->{name}, ($poll_id - ($poll_ids_back + 1)));
            }
            else {
                # Skip to the next host key here if the Poll ID is invalid
                $self->logger->debug("[_find_keys] No time available that matches the requested time!");
                next;
            }
        }

        try {
            $redis->get(
                $lookup,
                sub {
                    my ($reply, $err) = @_;
                    push(@$keys, $reply) if (defined ($reply));
                }
            );
        }
        catch ($e) {
            $self->logger->error("[_find_keys] Could not retrieve a set for $host from Redis: $_");
        };
    }

    # Wait for all of the Redis requests to return before continuing
    $redis->wait_all_responses();

    # Return the keys we found for each group
    return $keys;
}


=head2 _get()
    This will retrieve all SNMP data for a set of OIDs and hosts from Redis for a specific time period.
    Called as the main callback function for the Dispatcher's "get" method.
=cut
sub _get {

    my $self      = shift;
    my $requested = shift;
    my $params    = shift;
    my $rate_calc = shift;
    my $hosts     = $params->{'node'}{'value'};
    my $oidmatch  = $params->{'oidmatch'}{'value'};
    my $redis     = $self->redis;

    #$self->logger->debug("[_get] Processing get request for time " . $requested);

    # The hash of data we will return
    # {$host => $port(s) => {$oid(s) => {value => $value, time => $timestamp}}}
    my %results;

    # This is returned and used for rate calculation if needed
    my $prev_time;

    # Process every requested OID individually for each host
    for my $oid (@$oidmatch) {
        for my $host (@$hosts) {

            # Get all of the keys for the host and OID for the requested time period
            my $keys = $self->_find_keys(
                host      => $host,
                oid       => $oid,
                requested => $requested
            );

            $redis->select(0, sub{});

            # Skip the host for this OID if no keys for it were found
            if (!defined($keys) || (none {defined($_)} @$keys)) {
                $self->logger->error("[_get] Unable to find keys for $host -> $oid");
                next;
            }

            # Pull the SNMP data from Redis using each key
            for my $key (@$keys) {

                # Skip undefined keys
                next unless (defined($key));

                # Split the key into host, group, and timestamp
                my ($host_key, $group, $time) = split(',', $key);
                my ($host, $port, $context) = split(':', $host_key);

                # Set the time for the previous cycle to return for rate calculations
                if ($rate_calc && !defined($prev_time) && $time) {
                    $prev_time = $time - $params->{period}{value};
                }

                try {
                    # Scan Redis for hash entries, each scan will have a new set of entries
                    # Redis requires a cursor equal to zero to begin scanning
                    # It will return new cursor values with each call to sscan()
                    # Once Redis returns a cursor equal to zero, it's done iterating
                    my $entries;
                    my $cursor = 0;
                    while (1) {

                        # Get the set of hash entries that match our OID pattern
                        # COUNT will limit the number of entries returned per call to sscan()
                        ($cursor, $entries) = $redis->sscan(
                            $key, 
                            $cursor,
                            MATCH => $oid . "*",
                            COUNT => 2000
                        );

                        # Process each hash entry
                        for my $entry (@$entries) {

                            # Capture the OID and its polled Value from the entry string
                            my ($oid, $value) = $entry =~ /^([^,]*),(.*)/;

                            # Set the OID data for the host in our results
                            $results{$port}{$host}{$oid}{'value'} = $value;
                            $results{$port}{$host}{$oid}{'time'}  = $time;
                        }

                        # Redis has indicated that it finished all iterations
                        last if ($cursor == 0);
                    }
                }
                catch ($e) {
                    $self->logger->error("Error scanning Redis data for \"$key\": $e");
                };
            }
        }
    }

    # Wait for all pending responses to hmget requests
    $self->logger->debug("[_get] Waiting for all responses from Redis");
    $redis->wait_all_responses;
    $self->logger->debug("[_get] All responses are ready!");

    # Return the results we processed from Redis
    unless ($rate_calc) {
        return \%results;
    }
    # For rate calculations, return a timestamp for the previous poll cycle and the current results
    else {
        return [\%results, $prev_time];
    }
}


=head2 _rate()
    Calculates a requested value as a rate over the time period of an interval.
=cut
sub _rate {
    my ($self, $c_val, $c_time, $p_val, $p_time, $context) = @_;

    # Return the current value if it is NaN
    return $c_val unless ($c_val =~ /\d+$/);

    # Return 0 if there was none or negative time delta
    return 0 if ($p_time >= $c_time);

    # Determine the elapsed time delta
    my $delta = $c_time - $p_time;

    # Get the difference between current and previous value
    my $diff = $c_val - $p_val;

    # A negative difference means that a 32 or 64-bit counter reset itself to 0 after reaching the maximum
    if ($diff < 0) {
        $self->logger->debug("[_rate] Detected counter wrapping: $context") if (defined($context));
    
        # Determine whether the counter max value is a 32 or 64-bit unsigned integer
        # Then, set the difference as the difference between the max and previous value, plus the current value
        if ($c_val > MAX_UINT32 || $p_val > MAX_UINT32) {
            $diff = (MAX_UINT64 - $p_val) + $c_val;
        }
        else {
            $diff = (MAX_UINT32 - $p_val) + $c_val;
        }
    }

    # Calculate the rate as the value difference over the change in time
    my $rate = $diff / $delta;

    # A massive rate of change indicates invalid values.
    # We arbitrarily define massive as "1 Trillion" here.
    if ($rate > 1000000000000) {
        return undef;
    }
    else {
        return $rate;
    }
}

sub _get_rate {
    my $self   = shift;
    my $params = shift;
    my $redis  = $self->redis;

    if (!defined($params->{'period'}{'value'})) {
        $params->{'period'}{'value'} = 60;
        $self->logger->error("Rate value was requested, but no interval was given! Using default interval of 60s");
    }

    # Get data for the current and previous poll cycle
    my $current = $self->_get(time(), $params, 1);
    my $last    = $current->[1];
    my $c_data  = $current->[0];
    my $p_data  = $self->_get($last, $params);

    # The results hash to return
    # We return early if there isn't previous data to allow rate calculations
    my %results;
    return \%results unless (defined($p_data));

    # Iterate over current and previous data to calculate rate values
    while (my ($host, $ports) = each(%$c_data)) {
        while (my ($port, $oids) = each(%$ports)) {
            while (my ($oid, $data) = each(%$oids)) {
            
                my $c_val  = $data->{value};
                my $c_time = $data->{time};
                my $p_val  = $p_data->{$host}{$port}{$oid}{value};
                my $p_time = $p_data->{$host}{$port}{$oid}{time};

                # Check that current and previous values are defined numbers
                # Skip rate calculation for the combination if not
                next if (   
                       !defined $c_val
                    || !defined $p_val
                    || !defined $c_time
                    || !defined $p_time
                    || !looks_like_number($c_val)
                    || !looks_like_number($p_val)
                );

                # Set the returned results to the calculated rate value using the current time
                $results{$host}{$port}{$oid}{value} = $self->_rate($c_val, $c_time, $p_val, $p_time, sprintf("%s->%s->%s",$host,$port,$oid));
                $results{$host}{$port}{$oid}{time}  = $c_time;
            }
        }
    }
    return \%results;
}

1;