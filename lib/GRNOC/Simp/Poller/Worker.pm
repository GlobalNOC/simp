package GRNOC::Simp::Poller::Worker;

use strict;
use warnings;

use AnyEvent;
use AnyEvent::SNMP;
use Data::Dumper;
use JSON;
use Moo;
use Net::SNMP::XS;    # Faster than non-XS
use Redis::Fast;
use Syntax::Keyword::Try;

# Raised from 64, Default value
$AnyEvent::SNMP::MAX_RECVQUEUE = 128;

### Required Attributes ###
=head1 public attributes
=over 12

=item group_name
=item instance
=item config
=item logger
=item hosts
=item status_dir
=item oids
=item interval

=back
=cut

has group_name => (
    is       => 'ro',
    required => 1
);
has instance => (
    is       => 'ro',
    required => 1
);
has config => (
    is       => 'ro',
    required => 1
);
has logger => (
    is       => 'rwp',
    required => 1
);
has hosts => (
    is       => 'ro',
    required => 1
);
has status_dir => (
    is       => 'ro',
    required => 1
);
has oids => (
    is       => 'ro',
    required => 1
);
has interval => (
    is       => 'ro',
    required => 1
);

### Internal Attributes ###
=head1 private attributes
=over 12

=item worker_name
=item is_running
=item need_restart
=item redis
=item retention
=item request_size
=item timeout
=item main_cv
=item first_run

=back
=cut

has worker_name => (
    is       => 'rwp',
    required => 0,
    default  => 'unknown'
);
has is_running => (
    is      => 'rwp',
    default => 0
);
has need_restart => (
    is      => 'rwp',
    default => 0
);
has redis => (is => 'rwp');
has retention => (
    is      => 'rwp',
    default => 5
);
has request_size => (
    is      => 'rwp',
    default => 1
);
has timeout => (
    is      => 'rwp',
    default => 5
);
has main_cv => (is => 'rwp');
has first_run => (
    is      => 'rwp',
    default => 1
);

### Public Methods ###

=head1 methods
=over 12
=cut
=item start
=cut

sub start {
    my ($self) = @_;

    $self->_set_worker_name($self->group_name . ' [' . $self->instance . ']');

    my $logger = GRNOC::Log->get_logger($self->worker_name);
    $self->_set_logger($logger);

    my $worker_name = $self->worker_name;
    $self->logger->error(sprintf("%s - Starting...", $self->worker_name));

    # Flag that we're running
    $self->_set_is_running(1);

    # Change our process name
    $0 = "simp_poller($worker_name)";

    # Setup signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info($self->worker_name . " - Received SIG TERM.");
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        $self->logger->info($self->worker_name . " - Received SIG HUP.");
    };

    # Connect to redis
    my $redis_host      = $self->config->get('/config/redis/@ip')->[0];
    my $redis_port      = $self->config->get('/config/redis/@port')->[0];
    my $reconnect       = $self->config->get('/config/redis/@reconnect')->[0];
    my $reconnect_every = $self->config->get('/config/redis/@reconnect_every')->[0];
    my $read_timeout    = $self->config->get('/config/redis/@read_timeout')->[0];
    my $write_timeout   = $self->config->get('/config/redis/@write_timeout')->[0];

    $self->logger->debug(
        $self->worker_name . 
        " Connecting to Redis $redis_host:$redis_port" .
        " (reconnect = $reconnect," .
        " every = $reconnect_every," .
        " read_timeout = $read_timeout," .
        " write_timeout = $write_timeout)"
    );

    my $redis;
    try {
        # Try to connect twice per second for 30 seconds,
        # 60 attempts every 500ms.
        $redis = Redis::Fast->new(
            server        => "$redis_host:$redis_port",
            reconnect     => $reconnect,
            every         => $reconnect_every,
            read_timeout  => $read_timeout,
            write_timeout => $write_timeout
        );
    }
    catch ($e) {
        $self->logger->error(sprintf("%s - Error connecting to Redis: %s", $self->worker_name, $e));
    };

    $self->_set_redis($redis);

    $self->_set_need_restart(0);

    $self->logger->debug(sprintf('%s - Starting SNMP Poll loop', $self->worker_name));

    # Establish all of the SNMP sessions for every host the worker has
    $self->_connect_to_snmp();

    unless (scalar(@{$self->hosts})) {
        $self->logger->debug(sprintf("%s - No hosts found!", $self->worker_name));
    }
    else {
        my $host_names = join(', ', (map {$_->{name}} @{$self->hosts}));
        $self->logger->debug(sprintf("%s - hosts: %s", $self->worker_name, $host_names));
    }

    # Start AnyEvent::SNMP's max outstanding requests window equal to the total
    # number of requests this process will be making. AnyEvent::SNMP
    # will scale from there as it observes bottlenecks
    # TODO: THIS WILL NEED TO FACTOR IN HOST SESSIONS USING THEIR LOADS, NOT JUST HOST COUNT
    if (scalar(@{$self->oids}) && scalar(@{$self->hosts})) {
        AnyEvent::SNMP::set_max_outstanding(scalar(@{$self->hosts}) * scalar(@{$self->oids}));
    }
    else {
        $self->logger->error(sprintf("%s - Hosts or OIDs were not defined!", $self->worker_name));
        return;
    }

    $self->{'collector_timer'} = AnyEvent->timer(
        after    => 10,
        interval => $self->interval,
        cb       => sub {
            $self->_collect_data();
            AnyEvent->now_update;
        }
    );

    # Let the magic happen
    my $cv = AnyEvent->condvar;
    $self->_set_main_cv($cv);
    $cv->recv;
    $self->logger->error("Exiting");
}

=item stop
=back
=cut
sub stop {
    my $self = shift;
    $self->logger->error("Stop was called");
    $self->main_cv->send();
}

sub _poll_cb {
    my $self   = shift;
    my %params = @_;

    # Set params
    my $session    = $params{'session'};
    my $host       = $params{'host'};
    my $host_name  = $params{'host_name'};
    my $req_time   = $params{'timestamp'};
    my $reqstr     = $params{'reqstr'};
    my $main_oid   = $params{'oid'};
    my $context_id = $params{'context_id'};

    $self->logger->debug("Poll Callback Session:\n".Dumper($session));

    my $redis     = $self->redis;
    my $id        = $self->group_name;
    my $data      = $session->var_bind_list();
    my $timestamp = $req_time;

    my $ip      = $host->{'ip'};
    my $poll_id = $host->{'poll_id'};

    $self->logger->debug("_poll_cb running for $host_name: $main_oid");

    # Reset the pending reply status of the OID for the host
    # This advances pending replies even if we didn't get anything back for the OID
    # We do this because the polling cycle has completed in both cases
    my $pending_reply = defined($context_id) ? sprintf('%s,%s', $main_oid, $context_id) : $main_oid;
    delete $host->{'pending_replies'}{$pending_reply};

    # Parse the values from the data for Redis as an array of "oid,value" strings
    my @values = map {sprintf("%s,%s", $_, $data->{$_})} keys(%$data) if ($data);

    # Handle errors where there was no value data for the OID 
    unless (@values) {
        $host->{failed_oids}{$main_oid} = {
            error     => "OID_DATA_ERROR",
            timestamp => time()
        };
        $host->{failed_oids}{$main_oid}{context} = $context_id if (defined($context_id));

        my $error = $session->error();
        $self->logger->error("Host/Context ID: $host_name " . (defined($context_id) ? $context_id : '[no context ID]'));
        $self->logger->error("Group \"$id\" failed: $reqstr");
        $self->logger->error("Error: $error");
    }

    # Determine the expiration date for the data in Redis
    my $expiration = $timestamp + $self->retention;

    # Special key for the interval of the group (used by DB[3] to declare host variables)
    my $group_interval = sprintf("%s,%s", $self->group_name, $self->interval);

    # These hashes hold all of the keys for Redis for each DB entry
    my %host_keys = (
        db0 => sprintf("%s,%s,%s", $host_name, $self->worker_name, $timestamp),
        db1 => sprintf("%s,%s,%s", $host_name, $self->group_name, $poll_id),
        db2 => sprintf("%s,%s", $host_name, $self->group_name),
        db3 => sprintf("%s,%s", $host_name, $poll_id)
    );
    my %ip_keys = (
        db0 => sprintf("%s,%s,%s", $ip, $self->worker_name, $timestamp),
        db1 => sprintf("%s,%s,%s", $ip, $self->group_name, $poll_id),
        db2 => sprintf("%s,%s", $ip, $self->group_name),
        db3 => sprintf("%s,%s", $ip, $poll_id
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
        # Specifying the callback causes Redis piplining to reduce RTTs for this
        $redis->select(0, sub{});

        # Assign our value data from SNMP polling
        $redis->sadd($host_keys{db0}, @values, sub{}) if (@values);

        # Check that there are no pending replies left
        if (scalar(keys(%{$host->{'pending_replies'}})) == 0) {

            # Set the expiration time for the master key once all replies are received
            $redis->expireat($host_keys{db0}, $expiration, sub{});

            # Create keys for the Host, IP and Time
            my $time_key = sprintf("%s,%s", $poll_id, $timestamp);

            # Set the Poll ID => Timestamp lookup table data and its expiration
            $redis->select(1, sub{});
            $redis->set(     $host_keys{db1}, $host_keys{db0}, sub{});
            $redis->set(       $ip_keys{db1}, $ip_keys{db0},   sub{});
            $redis->expireat($host_keys{db1}, $expiration,     sub{});
            $redis->expireat(  $ip_keys{db1}, $expiration,     sub{});

            # Set the Host/IP => Timestamp lookup table data
            $redis->select(2, sub{});
            $redis->set(     $host_keys{db2}, $time_key,   sub{});
            $redis->set(       $ip_keys{db2}, $time_key,   sub{});
            $redis->expireat($host_keys{db2}, $expiration, sub{});
            $redis->expireat(  $ip_keys{db2}, $expiration, sub{});

            # Add any host variables to the SNMP data in Redis as "vars.$variable,$value"
            if (exists($host->{host_variable}) && defined($host->{host_variable})) {

                $self->logger->debug("Adding host variables for $host_name");

                # Set Redis back to DB[0] to insert the var data with values
                $redis->select(0, sub{});

                # Add each host variable and its value (remove commas from var name)
                while(my ($host_var, $value) = each(%{$host->{host_variable}})) {
                    $host_var = ($host_var =~ s/,//gr);
                    $redis->sadd($host_keys{db0}, sprintf("vars.%s,%s", $host_var, $value->{value}), sub{});
                }

                # Set the host variable's interval lookup 
                $redis->select(3, sub{});
                $redis->hset($host_name, "vars", $group_interval, sub{});
                $redis->hset($ip,        "vars", $group_interval, sub{});
            }

            # Increment the host's poll_id
            # TODO: Poll IDs will need to be stored by SNMP session, not host
            $host->{'poll_id'}++;
        }

        # Set an OID lookup using vars and interval for the OIDs
        $redis->select(3, sub{});
        $redis->hset($host_name, $main_oid, $group_interval, sub{});
        $redis->hset($ip,        $main_oid, $group_interval, sub{});
    }
    catch ($e) {
        $self->logger->error($self->worker_name . " $id Error in hset for data: $e");
    };

    # Wait for all async responses from Redis to complete before continuing
    $redis->wait_all_responses();

    # Update the AnyEvent pipeline
    AnyEvent->now_update;
}

sub _get_session_args {
    my $self = shift;
    my $host = shift;

    # Initialize the session args using ones always present
    my %session_args = (
        -hostname    => $host->{ip},
        -version     => $host->{snmp_version},
        -nonblocking => 1,
        -retries     => 5,
        -translate   => [-octetstring => 0],
        -maxmsgsize  => 65535,
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


=head2 _connect_to_snmp
    Uses host configs to generate SNMP args and create an SNMP session for every host.
    Stores the actual SNMP sessions in session hashes containing additional error and session info.
=cut
sub _connect_to_snmp {
    my $self  = shift;

    for my $host_attr (@{$self->hosts}) {

        # We will store our SNMP session objects in here for the host
        my @host_sessions;

        # Get the hostname, SNMP version, array of session args, and failed session tracker
        my $host_name    = $host_attr->{name};
        my $context_ids  = $host_attr->{contexts};
        my $version      = $host_attr->{snmp_version};
        my $session_args = $self->_get_session_args($host_attr);
        my $failed       = 0;

        # Get the current poll ID for the host from Redis
        # Initializes and Defaults to 0
        my $poll_id;
        try {
            $self->redis->select(2);
            $poll_id = $self->redis->get($host_name . ",main_oid");
        }
        catch ($e) {
            $self->logger->error("Error fetching the current poll cycle ID from Redis: $e");
            $host_attr->{errors}{redis} = {
                time  => time,
                error => "Error fetching the current poll cycle id from Redis: $e"
            };
            $self->redis->select(0);
        };
        $host_attr->{poll_id} = $poll_id || 0;

        # Create the session data hashes for each session
        # Apply the session args that were generated, multiplying by context IDs
        # If we don't have any contexts, pretend we do to avoid duplicate code
        for my $args (@$session_args) {

            # This will allow the next loop to run when there are not context IDs
            my $total_contexts = defined($context_ids) ? scalar(@$context_ids) : 1;

            for (my $i = 0; $i < $total_contexts; $i++ ) {

                my $context_id = $context_ids->[$i];

                # We store the SNMP session, port, context, and error data in this hash
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
                    $self->logger->error("SNMP is v1 or v2, but no community is defined for $host_name!");
                    $session_data{errors}{community} = {
                        time  => time,
                        error => "No community was defined for $host_name:"
                    };
                }


                # Connect the SNMP session and add it to the session hash
                my ($snmp_session, $session_error) = Net::SNMP->session(%$args);
                $session_data{session}  = $snmp_session;
            
                # Add the session hash to the host's sessions array
                push(@host_sessions, \%session_data);

                # Check for errors creating the SNMP session
                if (!$snmp_session || $session_error) {

                    my $err_str = "%s - Error while creating SNMPv%s session with %s:%s (ContextID: %s): %s";
                    my $cid_str = exists($session_data{context}) ? $session_data{context} : 'N/A';

                    my $error = sprintf(
                        $err_str,
                        $self->worker_name,
                        $version,
                        $host_name,
                        $session_data{port},
                        $cid_str,
                        $session_error
                    );

                    $self->logger->error($error);
            
                    $session_data{errors}{session} = {
                        time  => time,
                        error => $error
                    };
                }
            }
        }

        # Add the array of sessions to the worker's session hash
        $host_attr->{sessions} = \@host_sessions;
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
        $self->logger->error("Could not find dir for monitoring data: $mon_dir");
        return;
    }

    # Add filename to the path for writing
    $mon_dir .= $self->group_name . "_status.json";

    my %mon_data = (
        timestamp   => time(),
        failed_oids => scalar($host->{failed_oids}) ? $host->{failed_oids} : '',
        snmp_errors => scalar($host->{snmp_errors}) ? $host->{snmp_errors} : '',
        config      => $self->config->{config_file},
        interval    => $self->interval
    );
    $self->logger->debug(Dumper(\%mon_data));

    $self->logger->debug("Writing status file $mon_dir");

    # Write out the status file
    if (open(my $fh, '>:encoding(UTF-8)', $mon_dir)) {
        print $fh JSON->new->pretty->encode(\%mon_data);
        close($fh) || $self->logger->error("Could not close $mon_dir");
    }
    else {
        $self->logger->error("Could not open " . $mon_dir);
        return;
    }

    $self->logger->debug("Writing completed for status file $mon_dir");
}

sub _collect_data {
    my $self = shift;

    my $hosts     = $self->hosts;
    my $oids      = $self->oids;
    my $timestamp = time;

    $self->logger->debug("----  START OF POLLING CYCLE FOR: \"" . $self->worker_name . "\"  ----");

    $self->logger->debug(sprintf(
        "%s - %s hosts and %s oids per host, max outstanding scaled to %s and queue is %s to %s",
        $self->worker_name,
        scalar(@$hosts),
        scalar(@$oids),
        $AnyEvent::SNMP::MAX_OUTSTANDING,
        $AnyEvent::SNMP::MIN_RECVQUEUE,
        $AnyEvent::SNMP::MAX_RECVQUEUE
    ));
       
    for my $host (@$hosts) {

        my $host_name = $host->{name};

        # Retrieve any context IDs if present
        my $context_ids = $host->{contexts} if (exists($host->{contexts}));

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
                $self->logger->info(sprintf(
                    "%s - Unable to query device %s:%s in poll cycle, remaining oids: %s",
                    $self->worker_name,
                    $host->{ip},
                    $host_name,
                    $self->group_name,
                    Dumper($host->{pending_replies})
                ));
                next;
            }

            # Get the open SNMP session object
            my $snmp_session = $session->{session};

            if (!defined($snmp_session)) {
                $self->logger->error("$self->worker_name - No SNMP session defined for $host_name");
                next;
            }

            for my $oid_attr (@$oids) {
                my $oid     = $oid_attr->{oid};
                my $is_leaf = $oid_attr->{single} || 0;

                # Log an error if the OID is failing
                if (exists $session->{'failed_oids'}->{$oid}) {
                    $self->logger->error($session->{'failed_oids'}->{$oid}->{'error'});
                }

                my $reqstr = " $oid -> $host->{ip}:$session->{port} ($host_name)";

                # Determine which SNMPGET method we should use
                my $get_method = $is_leaf ? "get_request" : "get_table";

                # Iterate through the the provided set of base OIDs to collect
                my $res;
                my $res_err = sprintf("%s - Unable to issue Net::SNMP method \"%s\"", $self->worker_name, $get_method);

                # Create a hash of arguments for the request
                my %args = (-delay => $delay++);
                if ($is_leaf) {
                    $args{-varbindlist} = [$oid];
                }
                else {
                    $args{-baseoid}        = $oid;
                    $args{-maxrepetitions} = $self->request_size;
                }

                # Callback without context ID
                if (!exists($session->{context})) {
                    $args{-callback} = sub {
                        my $session = shift;
                        $self->_poll_cb(
                            host      => $host,
                            host_name => $host_name,
                            timestamp => $timestamp,
                            reqstr    => $reqstr,
                            oid       => $oid,
                            session   => $session
                        );
                    };
                }
                # Callback with context ID
                else {
                    $args{'-contextengineid'} = $session->{context};
                    $args{'-callback'} = sub {
                        my $session = shift;
                        $self->_poll_cb(
                            host       => $host,
                            host_name  => $host_name,
                            timestamp  => $timestamp,
                            reqstr     => $reqstr,
                            oid        => $oid,
                            context_id => $session->{context},
                            session    => $session
                        );
                    };
                }

                # Use the SNMP session to request data using our args
                $self->logger->debug($self->worker_name . " requesting " . $reqstr . " with method $get_method");
                $res = $snmp_session->$get_method(%args);

                if ($res) {
                    # Add the OID to pending replies if the request succeeded
                    $session->{pending_replies}->{$oid} = 1;
                }
                else {
                    # Add oid and info to failed_oids if it failed during request
                    $session->{'failed_oids'}{$oid} = {
                        error       => "OID_REQUEST_ERROR",
                        description => $snmp_session->error(),
                        timestamp   => time()
                    };
                    $self->logger->error("$res_err: " . $snmp_session->error());
                }
            }
        }
    }
    $self->logger->debug("----  END OF POLLING CYCLE FOR: \"" . $self->worker_name . "\"  ----");

    if ($self->first_run) {
        $self->_set_first_run(0);
    }
}

1;
