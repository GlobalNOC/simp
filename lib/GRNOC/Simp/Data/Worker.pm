package GRNOC::Simp::Data::Worker;

use strict;
use warnings;

use Carp;
use Data::Dumper;
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

    # Get Redis configuration variables
    my $redis_host            = $self->config->get('/config/redis/@ip');
    my $redis_port            = $self->config->get('/config/redis/@port');
    my $redis_reconnect       = $self->config->get('/config/redis/@reconnect');
    my $redis_reconnect_every = $self->config->get('/config/redis/@reconnect_every');
    my $redis_read_timeout    = $self->config->get('/config/redis/@read_timeout');
    my $redis_write_timeout   = $self->config->get('/config/redis/@write_timeout');

    # Get RabbitMQ configuration variables
    my $rabbit_host = $self->config->get('/config/rabbitmq/@ip');
    my $rabbit_port = $self->config->get('/config/rabbitmq/@port');
    my $rabbit_user = $self->config->get('/config/rabbitmq/@user');
    my $rabbit_pass = $self->config->get('/config/rabbitmq/@password');

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

    my $redis = Redis::Fast->new(
        server        => sprintf("%s:%s", $redis_host, $redis_port),
        reconnect     => $redis_reconnect,
        every         => $redis_reconnect_every,
        read_timeout  => $redis_read_timeout,
        write_timeout => $redis_write_timeout
    );
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

    # Try to get every field=>value combo for the hostname key from DB[3]
    # Data is returned with this structure: { OID => "group,interval" }
    my %oid_to_group;
    my $redis_err;
    try {
        $redis->select(3);
        %oid_to_group = $self->redis->hgetall($host);
        $self->logger->debug(Dumper(\%oid_to_group));
    }
    catch ($e) {
        $redis_err = 1;
        $self->logger->error("[_find_groups] Error fetching all groups $host is a part of: $e");
    };

    # Return if Redis had an error
    return if ($redis_err);

    # Process the host groups from what Redis returned
    my @host_groups;
    my @host_groups_rev;
    for my $base_oid (keys(%oid_to_group)) {

        # Parse the group name and interval from the value string for the OID
        my ($group, $interval) = split(',', $oid_to_group{$base_oid});

        # Group match if the base OID is a prefix of the requested OID
        if (_is_oid_prefix($base_oid, $oid)) {
            push(@host_groups, {base_oid => $base_oid, group => $group, interval => $interval});
        }
        # Group match if the requested OID is a prefix of the base_oid for reverse lookups
        elsif (_is_oid_prefix($oid, $base_oid)) {
            push(@host_groups_rev, {base_oid => $base_oid, group => $group, interval => $interval});
        }
    }

    # Reference to the array of groups we want to return
    my $groups;

    # Set the best matching host group if any were found
    if (scalar(@host_groups) >= 1) {
        $groups = $self->_best_host_groups(\@host_groups);
    }
    # Set the best matches from a reverse lookup
    else {
        # If we got here, we had no groups where $base_oid was a prefix of the OID tree we're interested in.
        # This might be because there are one or more groups whose base OID has $oid as a prefix!
        # We now look for these. If we find more than one, we then remove groups that are redundant,
        # i.e., groups for which there is a shorter-prefix group with the same or shorter interval.
        $groups = $self->_best_host_groups(\@host_groups_rev, 1);
    }

    # Log an error if no groups were defined
    $self->logger->error("[_find_groups] No host groups found for $host and $oid") unless ($groups);

    $self->logger->debug(Dumper($groups));
    return $groups;
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
    my $redis     = $self->redis;

    # Get the host's collection groups from Redis
    my $groups = $self->_find_groups(
        host      => $host,
        oid       => $oid,
    );

    # Ensure that groups were found for the host and OID
    if (!defined($groups) || (none {defined($_)} @$groups)) {
        $self->logger->error("[_find_keys] Unable to find groups for $host -> $oid");
        return;
    }
    
    # Array of keys for the sets we want to scan through per host+group
    my $keys = [];

    # Array of response objects containing hostgroup keys for the group
    my $hostgroup_keys = [];

    # Select the hostgroup DB in Redis
    $redis->select(2, sub{});

    # Try to find keys by iterating over the array of possible groups from _find_groups()
    for my $group (@$groups) {

        # Skip undefined groups
        next if !defined($group);

        # Get the Poll ID & Timestamp for the hostgroup combination asyncronously
        try {
            # Get the poll_id,timestamp string using the "host,group" key asynchronously
            $redis->get(
                sprintf("%s,%s", $host, $group->{'group'}),
                sub {
                    my $g = shift;
                    return sub {
                        my ($reply, $err) = @_;
                        push(@$hostgroup_keys, {'result' => $reply, 'group' => $g}) if ($reply);
                    };
                }->($group)
            );
        }
        catch ($e) {
            $self->logger->error("[_find_keys] Could not retrieve Poll ID/Timestamp data for $host -> $group->{group}: $e");
            next;
        };
    }
    
    # Wait until all the group keys have been returned
    $redis->wait_all_responses();

    # Select the keys DB in Redis 
    $redis->select(1, sub{});

    # Lookup the returned hostgroup keys from earlier in the keys DB of redis
    for my $hostgroup (@$hostgroup_keys) {

        # Pull the hostgroup key and group name from the reply
        my $res   = $hostgroup->{result};
        my $group = $hostgroup->{group};
    
        # Get the Poll ID and Timestamp from the returned value for the group
        my ($poll_id, $time) = split(',', $res);

        # Set a lookup key if our Poll ID is valid for the current time
        my $lookup;
        if ($requested > $time) {
            $lookup = sprintf("%s:*,%s,%s", $host, $group->{'group'}, $poll_id);
        }
        else {

            # Determine the most recent poll cycle
            my $poll_ids_back = floor(($time - $requested) / $group->{'interval'});

            # Set the lookup if the retrieved Poll ID is the same or newer than the closest poll cycle 
            if ($poll_id >= $poll_ids_back) {
                $lookup = sprintf("%s:*,%s,%s", $host, $group->{'group'}, ($poll_id - ($poll_ids_back + 1)));
            }
            else {
                # Return here if the Poll ID is invalid
                $self->logger->debug("[_find_keys] No time available that matches the requested time!");
                return;
            }
        }
        $self->logger->debug(sprintf("Finding keys from lookup: %s", $lookup));

        try {
            my $entries;
            my $cursor = 0;

            while (1) {

                # Get the set of hash entries that match our lookup pattern
                # COUNT will limit the number of entries returned per call to sscan()
                ($cursor, $entries) = $redis->scan(
                    $cursor,
                    MATCH => $lookup,
                    COUNT => 2000
                );

                # Add the returned entries to the keys array
                push(@$keys, @$entries);

                # Redis has indicated that it finished all iterations
                last if ($cursor == 0);
            }
        }
        catch ($e) {
            $self->logger->error("[_find_keys] Could not retrieve a set for $host from Redis: $_");
        };
    }

    # Wait for all of the Redis requests to return before continuing
    $redis->wait_all_responses();

    $self->logger->debug(sprintf("Keys from lookup: %s", Dumper($keys)));

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
    my $hosts     = $params->{'node'}{'value'};
    my $oidmatch  = $params->{'oidmatch'}{'value'};
    my $redis     = $self->redis;

    $self->logger->debug("[_get] Processing get request for time " . $requested);

    # The hash of data we will return
    # {$host => {$oid => {value => $value, time => $timestamp}}}
    my %results;

    # Switch back to DB 0 for the sscan's
    #$self->redis->select(0);

    # Process every requested OID individually for each host
    for my $oid (@$oidmatch) {
        for my $host (@$hosts) {

            # Get all of the keys for the host and OID for the requested time period
            my $keys = $self->_find_keys(
                host      => $host,
                oid       => $oid,
                requested => $requested
            );

            $self->redis->select(0);

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

                $self->logger->debug(sprintf(
                    "DATA LOOKUP PARAMS\nGROUP: %s\nTIME: %s\nHOST: %s\nPORT: %s\nCONTEXT: %s\n",
                    $group,
                    $time,
                    $host,
                    $port,
                    $context
                ));

                try {
                    $self->logger->debug('TRYING TO GET DATA FOR: '.$key);
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
                        $self->logger->debug(Dumper($entries));

                        # Process each hash entry
                        for my $entry (@$entries) {

                            # Capture the OID and its polled Value from the entry string
                            my ($oid, $value) = $entry =~ /^([^,]*),(.*)/;

                            # Set the OID data for the host in our results
                            $results{$host}{$port}{$oid}{'value'} = $value;
                            $results{$host}{$port}{$oid}{'time'}  = $time;
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
    $self->logger->debug(Dumper(\%results));
    return \%results;
}


=head2 _rate()
    Calculates a requested value as a rate over the time period of an interval.
=cut
sub _rate {
    my ($self, $cur_val, $cur_ts, $pre_val, $pre_ts, $context) = @_;

    # Return the current value if it is NaN
    return $cur_val unless ($cur_val =~ /\d+$/);

    # Return 0 if there was none or negative time delta
    return 0 if ($pre_ts >= $cur_ts);

    # Determine the elapsed time delta
    my $delta = $cur_ts - $pre_ts;

    # Get the difference between current and previous value
    my $diff = $cur_val - $pre_val;

    # A negative difference means that a 32 or 64-bit counter reset itself to 0 after reaching the maximum
    if ($diff < 0) {
        $self->logger->debug("[_rate] Detected counter wrapping: $context") if (defined($context));
    
        # Determine whether the counter max value is a 32 or 64-bit unsigned integer
        # Then, set the difference as the difference between the max and previous value, plus the current value
        if ($cur_val > MAX_UINT32 || $pre_val > MAX_UINT32) {
            $diff = (MAX_UINT64 - $pre_val) + $cur_val;
        }
        else {
            $diff = (MAX_UINT32 - $pre_val) + $cur_val;
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

    my %current_data;
    my %previous_data;

    if (!defined($params->{'period'}{'value'})) {
        $params->{'period'}{'value'} = 60;
    }

    #--- get the data for the current and a past poll cycle
    my $current_data = $self->_get(time(), $params);
    my $previous_data;

    my @ips = keys %{$current_data};
    if (defined($ips[0])) {
        my @oids = keys %{$current_data->{$ips[0]}};

        if (defined($oids[0])) {
            my $time = $current_data->{$ips[0]}{$oids[0]}{'time'};
            $time -= $params->{'period'}{'value'};

            $previous_data = $self->_get($time, $params);
        }
    }

    my %results;

    return \%results if !defined($previous_data);

    #iterate over current and previous data to calculate rates where sensible
    for my $ip (keys(%$current_data)) {
        for my $oid (keys(%{$current_data->{$ip}})) {
            my $current_val  = $current_data->{$ip}{$oid}{'value'};
            my $current_ts   = $current_data->{$ip}{$oid}{'time'};
            my $previous_val = $previous_data->{$ip}{$oid}{'value'};
            my $previous_ts  = $previous_data->{$ip}{$oid}{'time'};

            #--- sanity check
            if (   !defined $current_val
                || !defined $previous_val
                || !defined $current_ts
                || !defined $previous_ts) {

                #--- incomplete data
                next;
            }

            # Skip rate if it's NaN
            unless (looks_like_number($current_val)) {
                next;
            }

            $results{$ip}{$oid}{'value'} = $self->_rate($current_val, $current_ts, $previous_val, $previous_ts, "$ip->$oid");
            $results{$ip}{$oid}{'time'}  = $current_ts;

        }
    }
    return \%results;
}

1;
