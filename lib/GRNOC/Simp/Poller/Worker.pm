package GRNOC::Simp::Poller::Worker;

use strict;
use warnings;

# Enable for the -d:Trace option to allow stack tracing
#$Devel::Trace::TRACE = 1;

use AnyEvent;
use AnyEvent::SNMP;
use Data::Dumper;
use JSON;
use Moo;
use Try::Tiny;
use IPC::Shareable;
use Net::SNMP::XS;    # Faster than non-XS
use Redis::Fast;

# Raised from 64, Default value
$AnyEvent::SNMP::MAX_RECVQUEUE = 128;

### Required Attributes ###
=head1 public attributes
=over 12

=item parent
=item id
=item group
=item logger
=item status_dir

=back
=cut

has parent     => (is => 'rwp', required => 1);
has id         => (is => 'rwp', required => 1);
has group      => (is => 'rwp', required => 1);
has logger     => (is => 'rwp', required => 1);
has status_dir => (is => 'rwp', required => 1);

### Internal Attributes ###
=head1 private attributes
=over 12

=item name
=item hosts
=item oids
=item interval
=item redis
=item retention
=item request_size
=item timeout
=item event_loop
=item first_run

=back
=cut

has name         => (is => 'rwp');
has hosts        => (is => 'rwp', default => sub {[]});
has oids         => (is => 'rwp', default => sub {[]});
has interval     => (is => 'rwp');
has redis        => (is => 'rwp');
has redis_config => (is => 'rwp', default => sub {{}});
has retention    => (is => 'rwp', default => 5);
has request_size => (is => 'rwp', default => 1);
has timeout      => (is => 'rwp', default => 5);
has event_loop   => (is => 'rwp');
has first_run    => (is => 'rwp', default => 1);

### Public Methods ###

=head1 methods
=over 12
=cut
=item start
=cut


# Logging helpers that shorten calls and add the worker's name
sub worker_log {
    my ($self, $msg) = @_;
    return sprintf('%s: %s', $self->name, $msg)
}
sub log_debug {
    my ($self, $msg) = @_;
    $self->logger->debug($self->worker_log($msg));
}
sub log_info {
    my ($self, $msg) = @_;
    $self->logger->info($self->worker_log($msg));
}
sub log_error {
    my ($self, $msg) = @_;
    $self->logger->error($self->worker_log($msg));
}


=head2 _get_shared_config()
    Returns the configuration hash for $key from shared memory.
    Configurations are stored as JSON and are decoded upon retrieval.
=cut
sub _get_shared_config {
    my $self = shift;
    my $key  = shift;
 
    # Add the parent PID to the key for this instance
    $key = sprintf("%s:%s", $key, $self->parent);

    $self->log_debug("Retrieving '$key' config from shared memory"); 
    try { 
        tie(my $data_json, 'IPC::Shareable', {key => $key});
        return decode_json($data_json);
    } 
    catch {
        $self->log_error("ERROR: Could not get shared '$key' config: $_");
    };
}


=head2 _make_config()
    Wrapper method that runs all of the config-loading methods
=cut
sub _make_config {
    my $self = shift;

    $self->_make_redis();
    $self->_make_hosts();
    $self->_make_group();
    $self->_make_redis();

    $self->log_debug("Finished setting configurations");
}


=head2 _make_hosts()
    Creates the worker's hosts configuration from shared memory
=cut
sub _make_hosts {
    my $self = shift;
    my $hosts_config  = $self->_get_shared_config('hosts');
    my $worker_config = $self->_get_shared_config('workers');

    # Find the hosts assigned to the worker and get their configs
    my @hosts = map {$hosts_config->{$_}} @{$worker_config->{$self->group}{$self->id}{hosts}};

    # Apply group-specific configs for the hosts
    for my $host (@hosts) {
        my $group_configs = $host->{group}{$self->group};
        
        $self->log_error("No group-specific configs for ".$host->{name}) unless (defined($group_configs));
        
        # Copy values from the group config reference
        my @ports  = @{$group_configs->{ports}};
        my %errors = %{$group_configs->{errors}};

        # Apply the copied data to the host config
        $host->{ports}  = \@ports;
        $host->{errors} = \%errors;

        # Get rid of the group-specific host configs once applied locally
        delete $host->{group};
    }

    $self->_set_hosts(\@hosts);
    $self->log_info("Updated hosts configuration from shared memory");
}


=head2 _make_group()
    Create the polling group configuration from shared memory.
=cut
sub _make_group {
    my $self = shift;
    my $groups_config = $self->_get_shared_config('groups');

    my $group = $groups_config->{$self->group};

    $self->_set_oids($group->{oids});
    $self->_set_timeout($group->{timeout});
    $self->_set_interval($group->{interval});
    $self->_set_retention($group->{retention});
    $self->_set_request_size($group->{request_size});

    $self->log_info("Updated polling group configuration from shared memory");
}


=head2 _make_redis()
     Creates the Redis configuration from shared memory.
     Connects to the Redis server using the new config
=cut
sub _make_redis {
    my $self = shift;
    my $redis_config = $self->_get_shared_config('redis');

    my $msg  = sprintf("Connecting to Redis server at %s", $redis_config->{server});
    $msg .= ' (using unix socket)' if (defined($redis_config->{sock}));

    # Connect to Redis. Only continue once Redis is connected.
    # This will try to reconnect 2x per second for 30s
    my $redis;
    my $connected = 0;
    while (!$connected) {
        $self->log_info($msg);
        try {
            $redis = Redis::Fast->new(%$redis_config);
            $connected = 1;
        }
        catch {
            $self->log_error("ERROR: Couldn't connect to Redis: $_");
        };
    }

    $self->log_debug(sprintf(
        "Redis reconnect after %ss every %ss",
        $redis_config->{reconnect},
        $redis_config->{reconnect_every}
    ));
    $self->log_debug(sprintf(
        "Redis timeouts [Read: %ss, Write: %ss]",
        $redis_config->{read_timeout},
        $redis_config->{write_timeout}
    ));

    $self->_set_redis($redis);
}


=head2 start()
    Public start method called by the main Poller process.
    This sets up the worker and begins data collection.
=cut
sub start {
    my ($self) = @_;

    # Create the worker name using its polling group and worker ID
    # Set the process name to it and store it in the worker instance
    my $name = sprintf("%s [%s]", $self->group, $self->id);
    $0 = sprintf("simp_poller(%s)", $name);
    $self->_set_name($name);

    # Get the logger object
    my $logger = GRNOC::Log->get_logger($self->name);
    $self->_set_logger($logger);
    $self->log_info("Starting");

    # Setup signal handlers
    $SIG{'TERM'} = sub {
        $self->log_info("Received SIG TERM");
        $self->stop();
        return 0;
    };
    $SIG{'HUP'} = sub {
        $self->log_info("Received SIG HUP");
        $self->reload();
    };

    $self->_make_config();
    $self->_connect_to_snmp();

    $self->log_debug('Starting SNMP Poll loop');

    # Establish all of the SNMP sessions for every host the worker has
    $self->_connect_to_snmp();

    # Initialize AnyEvent::SNMP's max outstanding requests window.
    # Set it equal to the total number of requests this process makes. 
    # AnyEvent::SNMP will scale from there as it observes bottlenecks.
    # TODO: THIS WILL NEED TO FACTOR IN HOST SESSIONS USING THEIR LOADS, NOT JUST HOST COUNT
    if (scalar(@{$self->oids}) && scalar(@{$self->hosts})) {
        AnyEvent::SNMP::set_max_outstanding(scalar(@{$self->hosts}) * scalar(@{$self->oids}));
    }
    else {
        $self->log_error('No hosts in configuration') if (scalar(@{$self->hosts}) == 0);
        $self->log_error('No OIDs in configuration') if (scalar(@{$self->oids}) == 0);
        exit(1);
    }

    # Create the timer that collects data on every interval
    $self->{'collector_timer'} = AnyEvent->timer(
        interval => $self->interval,
        cb       => sub {
            $self->_collect_data();
            AnyEvent->now_update;
        }
    );

    # Let the magic happen
    my $cv = AnyEvent->condvar;
    $self->_set_event_loop($cv);
    $cv->recv;
    $self->log_info("Exiting");
}

=head2 stop()
    Stops the worker process.
=cut
sub stop {
    my $self = shift;
    $self->log_info("Stopping");
    $self->event_loop->send();
}


=head2 reload()
    Reloads the worker process.
=cut
sub reload {
    my $self = shift;
    $self->log_info("Reloading");
    $self->_make_config();
    $self->_connect_to_snmp();
}


sub _make_interval_timestamp {
    my $self = shift;
    my $time = time();

    my $new = $time % $self->interval
}


=head2 _poll_cb()
    A callback passed to SNMP GET/GETBULK methods.
    This runs after the SNMP request completes, independent of other code.
=cut
sub _poll_cb {
    my $self   = shift;
    my %params = @_;

    # Set the arguments for the callback
    my $session = $params{session};
    my $time    = $params{time};
    my $name    = $params{name};
    my $ip      = $params{ip};
    my $oid     = $params{oid};
    my $reqstr  = $params{reqstr};
    my $vars    = $params{vars};

    # Get some variables from the worker
    my $redis    = $self->redis;
    my $group    = $self->group;
    my $worker   = $self->name;

    # Get a reference to the SNMP session, port, and context
    my $snmp    = $session->{session};
    my $port    = $session->{port};
    my $context = $session->{context};

    # Get, find, or initialize the session's poll_id
    my $poll_id;
    my $poll_time;
    if (exists($session->{poll_id})) {
        $poll_id = $session->{poll_id};
    }
    else {
        # Get the current poll ID for the host from Redis
        try {
            # Poll IDs are stored in "$poll_id,$time" strings in DB[2]
            # They're retrieved with "$hostname:$port,$poll_group" keys
            $self->redis->wait_all_responses();
            $self->redis->select(2);
            my $poll_cycle = $self->redis->get(sprintf("%s:%s,%s", $name, $port, $group));
            ($poll_id, $poll_time) = split(',', $poll_cycle) if (defined($poll_cycle));
        }
        catch {
            $self->logger->error("Error fetching the current poll cycle ID from Redis for $name: $_");
            $self->redis->wait_all_responses();
            $self->redis->select(0);
        };

        # Initialize the poll_id to 0 if it wasn't found
        unless (defined($poll_id)) {
            $poll_id = 0;
            $poll_time = time();
            $self->logger->debug("Poll cycle ID not found, initializing it as cycle $poll_id at $poll_time");
        }
        else {
            $self->logger->debug("Poll cycle ID found, continuing from poll cycle $poll_id at $poll_time");
            $poll_id += 1;
        }

        # Set the poll_id for the session
        $session->{poll_id} = $poll_id;
    }

    $self->logger->debug(sprintf("%s - _poll_cb processing %s:%s %s", $worker, $name, $port, $oid));

    # Reset the session's pending reply status for the OID
    # This advances pending replies even if we didn't get anything back for the OID
    # We do this because the polling cycle has completed in both cases
    delete $session->{'pending_replies'}{$oid};

    # Get the data returned by the session's SNMP request
    my $data = $snmp->var_bind_list();

    # Parse the values from the data for Redis as an array of "oid,value" strings
    my @values = map {sprintf("%s,%s", $_, $data->{$_})} keys(%$data) if ($data);

    # Handle errors where there was no value data for the OID 
    unless (@values) {
        $session->{failed_oids}{"$oid:$port"} = {
            error     => "OID_DATA_ERROR",
            timestamp => time()
        };

        # Add the context to the hash of failed OIDs for the session
        $session->{failed_oids}{$oid}{context} = $context if (defined($context));

        my $error = "ERROR: %s failed to request %s: %s"; 
        $self->log_error(sprintf($error, $name, $reqstr, $snmp->error()));
    }

    # Calculate when this key should expire based on NOW when the response
    # is received offset against when it was sent, to keep all redis entry timeouts aligned
    my $expire = ($time + $self->retention) - time();

    # Special key for the interval of the group (used by DB[3] to declare host variables)
    my $group_interval = sprintf("%s,%s", $group, $self->interval);

    # Create a key from our hostname/ip, port, and context
    my $name_id = defined($context) ? "$name:$port:$context" : "$name:$port";
    my $ip_id   = defined($context) ? "$ip:$port:$context"   : "$ip:$port";

    # These hashes hold all of the keys for Redis for each DB entry
    my %host_keys = (
        db0 => sprintf("%s,%s,%s", $name_id, $group, $time),
        db1 => sprintf("%s,%s,%s", $name_id, $group, $poll_id),
        db2 => sprintf("%s,%s",    $name_id, $group),
        db3 => sprintf("%s,%s",    $name_id, $poll_id)
    );
    my %ip_keys = (
        db0 => sprintf("%s,%s,%s", $ip_id, $group, $time),
        db1 => sprintf("%s,%s,%s", $ip_id, $group, $poll_id),
        db2 => sprintf("%s,%s",    $ip_id, $group),
        db3 => sprintf("%s,%s",    $ip_id, $poll_id)
    );

    # Attempt to set the data for all of the Redis databases
    try {
        # REDIS DB SELECTOR MAPPINGS
        # Note: These DBs are used by SIMP-Data in decrementing order, starting at DB[3]
        #
        # DB[0]: {"$host,$worker,$time" => [Value Data]}
        # Used to store and lookup value data for OIDs
        #
        # DB[1]: {"$host,$group,$poll_id" => "$host,$worker,$time"} 
        # Used to retrieve keys to value data using the Host, Group, and Poll ID
        # Allows us to find keys to value data using keys generated using data from DB[3] and DB[2]
        #
        # DB[2]: {"$host,$group" => "$poll_id,$time"}
        # Used to map a Host and Group to the Poll ID and Timestamp of the Poll ID
        # Allows us to build a key for DB[1]
        #
        # DB[3]: {$hosts => { $oids => "$group,$interval"}}
        # Used to retrieve a hash of OIDs and their groups using only a hostname
        # Allows us to build keys for DB[2]

        # Select DB[0] of Redis so we can insert value data
        # Specifying the callback causes Redis piplining to reduce RTTs

        $redis->select(0, sub{});
        $redis->sadd($host_keys{db0}, @values, sub{}) if (@values);

        # Check that the session has no pending replies left
        if (scalar(keys(%{$session->{'pending_replies'}})) == 0) {

            # Set the expiration time for the master key once all replies are received
            $redis->expire($host_keys{db0}, $expire, sub{});

            # Create keys for the Host, IP and Time
            my $time_key = sprintf("%s,%s", $poll_id, $time);

            # Set the Poll ID => Timestamp lookup table data and its expiration
            $redis->select(1, sub{});
            $redis->setex($host_keys{db1}, $expire, $host_keys{db0}, sub{});
            $redis->setex($ip_keys{db1}, $expire, $ip_keys{db0}, sub{});

            # Set the Host/IP => Timestamp lookup table data
            $redis->select(2, sub{});
            $redis->setex($host_keys{db2}, $expire, $time_key, sub{});
            $redis->setex($ip_keys{db2}, $expire, $time_key, sub{});

            # Add any host-defined variables to the SNMP data in Redis as "vars.$var,$value"
            if (defined($vars)) {

                $self->log_debug("Adding host variables for $name");

                # Set Redis back to DB[0] to insert the var data with values
                $redis->select(0, sub{});

                # Add each host variable and its value (remove commas from var name)
                while(my ($var, $value) = each(%$vars)) {
                    $var = ($var =~ s/,//gr);
                    $redis->sadd($host_keys{db0}, sprintf("vars.%s,%s", $var, $value->{value}), sub{});
                }

                # Set the host variable's interval lookup 
                $redis->select(3, sub{});
                $redis->hset($name_id, "vars", $group_interval, sub{});
                $redis->hset($ip_id,   "vars", $group_interval, sub{});
            }

            # Increment the session's poll_id
            $session->{poll_id}++;
        }

        # Set an OID lookup using vars and interval for the OIDs
        $redis->select(3, sub{});
        $redis->hset($name_id, $oid, $group_interval, sub{});
        $redis->hset($ip_id,   $oid, $group_interval, sub{});
    }
    catch {
        $self->log_error("ERROR: could not hset Redis data: $_");
    };

    # Ensure we're done with the pipeline
    $redis->wait_all_responses();

    # Update the AnyEvent pipeline
    AnyEvent->now_update;
}


=head2 _get_session_args()
    Builds the parameters for a host's Net::SNMP session object.
=cut
sub _get_session_args {
    my $self = shift;
    my $host = shift;

    # Initialize the session args using ones always present
    my %session_args = (
        -hostname    => $host->{ip},
        -version     => $host->{snmp_version},
        -domain      => $host->{transport_domain},
        -nonblocking => 1,
        -retries     => 5,
        -translate   => [-octetstring => 0],
        -maxmsgsize  => 65535,
        -timeout     => $self->timeout,
    );

    # Optional session argument mappings to their host attribute from config
    my %optional_args = (
        '-community'    => 'community',
        '-username'     => 'username',
        '-authkey'      => 'auth_key',
        '-authpassword' => 'auth_password',
        '-authprotocol' => 'auth_protocol',
        '-privkey'      => 'priv_key',
        '-privpassword' => 'priv_password',
        '-privprotocol' => 'priv_protocol'
    );

    # Stores every set of session args needed for the host
    my @sessions;

    # Create a new set of session args for each port
    for my $port (@{$host->{ports}}) {
        
        # Copy the session args for each port
        my %session = %session_args;

        # Add each optional arg for the session if it's specified
        while (my ($arg, $attr) = each(%optional_args)) {
            $session{$arg} = $host->{$attr} if (exists($host->{$attr}) && defined($host->{$attr}));
        }

        # Add the port to the session args
        $session{'-port'} = $port;

        push(@sessions, \%session);
    }
    return \@sessions;
}


=head2 _connect_to_snmp()
    Uses host configs to generate SNMP args and create an SNMP session for every host.
    Stores the actual SNMP sessions in session hashes containing additional error and session info.
=cut
sub _connect_to_snmp {
    my $self  = shift;

    for my $host_attr (@{$self->hosts}) {

        # We will store our SNMP session objects in here for the host
        $host_attr->{sessions} = [];

        # Get the hostname, SNMP version, array of session args, and failed session tracker
        my $host_name    = $host_attr->{name};
        my $context_ids  = $host_attr->{context_id};
        my $version      = $host_attr->{snmp_version};
        my $session_args = $self->_get_session_args($host_attr);
        my $failed       = 0;

        # Create the session data hashes for each session
        # Apply the session args that were generated, multiplying by context IDs
        # If we don't have any contexts, pretend we do to avoid duplicate code
        for my $args (@$session_args) {

            $self->log_debug(sprintf("Creating session for %s:%s", $host_name, $args->{'-port'}));

            # This will allow the next loop to run when there are not context IDs
            my $total_contexts = defined($context_ids) ? scalar(@$context_ids) : 1;

            for (my $i = 0; $i < $total_contexts; $i++ ) {

                my $context_id = $context_ids->[$i];

                # We store the SNMP session, poll_id, port, context, and error data in this hash
                # Note: A poll_id will be set later during the first polling cycle in poll_cb().
                my %session_data = (
                    session         => 0,
                    port            => $args->{'-port'},
                    errors          => {},
                    failed_oids     => {},
                    pending_replies => {},
                ); 
                $session_data{context} = $context_id if (defined($context_id));
        
                # Send an error when there's no community specified and we have V1 or V2
                if ($version < 3 && !defined($args->{'-community'})) {
                    $self->log_error("SNMP is v1 or v2, but no community is defined for $host_name!");
                    $session_data{errors}{community} = {
                        time  => time,
                        error => "No community was defined for $host_name:"
                    };
                }

                # Connect the SNMP session and add it to the session hash
                my ($snmp_session, $session_error) = Net::SNMP->session(%$args);
                $session_data{session} = $snmp_session;
            
                # Add the session hash to the host's sessions array
                push(@{$host_attr->{sessions}}, \%session_data);

                # Check for errors creating the SNMP session
                if (!$snmp_session || $session_error) {

                    my $err_str = "Error while creating SNMPv%s session with %s:%s (ContextID: %s): %s";
                    my $cid_str = exists($session_data{context}) ? $session_data{context} : 'N/A';

                    my $error = sprintf(
                        $err_str,
                        $version,
                        $host_name,
                        $session_data{port},
                        $cid_str,
                        $session_error
                    );
                    $self->log_error($error);
            
                    $session_data{errors}{session} = {
                        time  => time,
                        error => $error
                    };
                }
            }
        }
    }
}


sub _write_mon_data {
    my $self = shift;
    my $host = shift;
    my $name = shift;

    # Skip writing if it's the worker's first poll cycle
    # to avoid clearing active alarm status'
    return if $self->first_run;

    # Set dir for writing status files to status_dir defined in simp-poller.pl
    my $mon_dir = $self->status_dir . $name . '/';

    # Checks if $mon_dir exists or creates it
    unless (-e $mon_dir) {

        # Note: If the dir can't be accessed due to permissions, writing will fail
        $self->log_error("Could not find dir for monitoring data: $mon_dir");
        return;
    }

    # Add filename to the path for writing
    $mon_dir .= $self->group . "_status.json";
    
    $self->log_debug("Writing status file $mon_dir");

    my %mon_data = (
        timestamp   => time(),
        failed_oids => '',
        snmp_errors => '',
        #config      => $self->config->{config_file},
        interval    => $self->interval
    );

    # All the error data for each session's failed OIDs
    my $failed_oids = {};

    # All of each session's SNMP errors
    # Note: 
    #   The community and version subsections are deprecated by config validation
    #   They are left in here for backward compatibility with poller-monitoring.
    my $snmp_errors = {
        session   => [],
        community => 0,
        version   => 0
    };
    my $session_errors = $snmp_errors->{session};

    for my $session (@{$host->{sessions}}) {
        for my $oid (keys($session->{pending_replies})) {
            $failed_oids->{"$oid:161"} = {
                timestamp => time() - $self->interval / 2,
                error     => 'OID_DATA_ERROR',
            };
        }
        for my $oid (keys($session->{failed_oids})) {
            $failed_oids->{$oid} = $session->{failed_oids}{$oid};
        }
        push(@{$session_errors}, $session->{errors}{session}) if ($session->{errors}{session});
    }

    $mon_data{snmp_errors} = $snmp_errors;
    $mon_data{failed_oids} = $failed_oids;

    # Write out the status file
    if (open(my $fh, '>:encoding(UTF-8)', $mon_dir)) {
        print $fh JSON->new->pretty->encode(\%mon_data);
        close($fh) || $self->log_error("Could not close $mon_dir");
    }
    else {
        $self->log_error("Could not open " . $mon_dir);
        return;
    }

    $self->log_debug("Writing completed for status file $mon_dir");
}


# Attempts to stop the worker from becoming a zombie
# Also cleans up shared memory when the parent has crashed
sub _zombie_control {
    my $self = shift;
   
    # Returns a true value if the parent PID is running
    # Does not work when child uid/gid is not the same as parent uid/gid
    my $running = kill(-0, $self->parent);

    unless ($running) {
        $self->log_error("Parent PID ".$self->parent." died, stopping zombification");

        # Remove shared memory segments and semaphores for this poller instance
        # Note: Other workers will error on this after the first one removes them.
        IPC::Shareable->clean_up_all();

        # Stop the zombie worker
        $self->stop(); 
    }
}


sub _collect_data {
    my $self      = shift;
    my $hosts     = $self->hosts;
    my $oids      = $self->oids;
    my $timestamp = time;

    # Exit if the parent process has died
    # This stops a zombie worker from running after a hard crash
    $self->_zombie_control();

    $self->log_debug("----  START OF POLLING CYCLE  ----");

    $self->log_debug(sprintf(
        "%s hosts and %s oids per host, max outstanding scaled to %s and queue is %s to %s",
        scalar(@$hosts),
        scalar(@$oids),
        $AnyEvent::SNMP::MAX_OUTSTANDING,
        $AnyEvent::SNMP::MIN_RECVQUEUE,
        $AnyEvent::SNMP::MAX_RECVQUEUE
    ));   

    for my $host (@$hosts) {

        my $host_name = $host->{name};

        # Retrieve any context IDs if present
        my $context_ids = $host->{context_id} if (exists($host->{context_id}));

        # Used to stagger each request to a specific host, spread load.
        # This does mean you can't collect more OID bases than your interval
        my $delay = 0;

        # Write mon data out for the host's last completed poll cycle
        $self->_write_mon_data($host, $host_name);

        for my $session (@{$host->{sessions}}) {

            # Reset the failed OIDs for each session after writing the status
            $session->{failed_oids} = {};

            # Log error and skip collections for the session if it has an
            # OID key in pending response with a val of 1
            if (scalar(keys %{$session->{'pending_replies'}}) > 0) {

                $self->log_info(sprintf(
                    "Unable to query device %s:%s in poll cycle, remaining oids: %s",
                    $host->{ip},
                    $host_name,
                    Dumper($session->{pending_replies})
                ));
                next;
            }

            # Get the open SNMP session object
            my $snmp_session = $session->{session};

            if (!defined($snmp_session)) {
                $self->log_error("No SNMP session defined for $host_name");
                next;
            }

            for my $oid_attr (@$oids) {

                my $oid     = $oid_attr->{oid};
                my $is_leaf = $oid_attr->{single} || 0;

                my $reqstr = " $oid -> $host->{ip}:$session->{port} ($host_name)";

                # Determine which SNMPGET method we should use
                my $get_method = $is_leaf ? "get_request" : "get_table";

                # Iterate through the the provided set of base OIDs to collect
                my $res;
                my $res_err = sprintf("Unable to issue Net::SNMP method \"%s\"", $get_method);

                # Create a hash of arguments for the request
                my %args = (-delay => $delay++);
                if ($is_leaf) {
                    $args{-varbindlist} = [$oid];
                }
                else {
                    $args{-baseoid}        = $oid;
                    $args{-maxrepetitions} = $self->request_size;
                }

                # Add the context to session args if present
                if (exists($session->{context})) {
                    $args{'-contextengineid'} = $session->{context};
                }

                # Define the callback with params for the session args
                $args{'-callback'} = sub {
                    $self->_poll_cb(
                        session    => $session,
                        time       => $timestamp,
                        name       => $host_name,
                        ip         => $host->{ip},
                        oid        => $oid,
                        reqstr     => $reqstr,
                        vars       => $host->{host_variable}
                    );
                }; 

                # Use the SNMP session to request data using our args
                $self->log_debug(sprintf("%s request for %s", $get_method, $reqstr));
                $res = $snmp_session->$get_method(%args);

                if ($res) {
                    # Add the OID to pending replies if the request succeeded
                    $session->{pending_replies}{$oid} = 1;
                }
                else {
                    # Add oid and info to failed_oids if it failed during request
                    $session->{'failed_oids'}{"$oid:$session->{port}"} = {
                        error       => "OID_REQUEST_ERROR",
                        description => $snmp_session->error(),
                        timestamp   => time()
                    };
                    $self->log_error(sprintf("%s: %s", $res_err, $snmp_session->error()));
                }
            }
        }
    }
    $self->log_debug("----  END OF POLLING CYCLE  ----");

    if ($self->first_run) {
        $self->_set_first_run(0);
    }
}

1;
