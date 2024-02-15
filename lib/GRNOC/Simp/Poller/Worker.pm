#!/usr/bin/perl -I /opt/grnoc/venv/simp/lib/perl5

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
=item status_data

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
has status_data => (
    is => 'rwp',
    default => sub{{}}
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

    my $worker = $self->worker_name;
    $self->logger->error(sprintf("%s - Starting...", $worker));

    # Flag that we're running
    $self->_set_is_running(1);

    # Change our process name
    $0 = "simp_poller($worker)";

    # Setup signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info($worker . " - Received SIG TERM.");
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        $self->logger->info($worker . " - Received SIG HUP.");
    };

    my $redis = $self->_connect_to_redis($worker);

    $self->_set_redis($redis);
    $self->logger->debug(sprintf('%s - Finished connecting to Redis', $worker));

    $self->_set_need_restart(0);

    $self->logger->debug(sprintf('%s - Starting SNMP Poll loop', $worker));

    # Establish all of the SNMP sessions for every host the worker has
    $self->_connect_to_snmp();

    unless (scalar(@{$self->hosts})) {
        $self->logger->debug(sprintf("%s - No hosts found!", $worker));
    }
    else {
        my $host_names = join(', ', (map {$_->{name}} @{$self->hosts}));
        $self->logger->debug(sprintf("%s - hosts: %s", $worker, $host_names));
    }

    # Start AnyEvent::SNMP's max outstanding requests window equal to the total
    # number of requests this process will be making. AnyEvent::SNMP
    # will scale from there as it observes bottlenecks
    my $host_count = scalar(@{$self->hosts});
    my $oid_count  = scalar(@{$self->oids});
    if ($host_count && $oid_count) {
        AnyEvent::SNMP::set_max_outstanding($host_count * $oid_count);
    }
    elsif ($host_count == 0) {
        $self->logger->error(sprintf("%s - No hosts assigned to worker", $worker));
        return;
    }
    elsif ($oid_count == 0) {
        $self->logger->error(sprintf("%s - No OIDs defined for polling group", $worker));
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
    $self->logger->error(sprintf("%s - Exiting", $worker));
}

=item stop
=back
=cut
sub stop {
    my $self = shift;
    $self->logger->error(sprintf("%s - Stop was called", $self->worker_name));
    $self->main_cv->send();
}

=head2 _connect_to_redis()
     Helper function that helps connecting to the redis server 
     and returns the redis object reference
=cut
sub _connect_to_redis {
    my ($self, $worker) = @_;

    # Connect to redis
    my $reconnect       = $self->config->get('/config/redis/@reconnect')->[0];
    my $reconnect_every = $self->config->get('/config/redis/@reconnect_every')->[0];
    my $read_timeout    = $self->config->get('/config/redis/@read_timeout')->[0];
    my $write_timeout   = $self->config->get('/config/redis/@write_timeout')->[0];

 
    my %redis_conf = (
        reconnect     => $reconnect,
        every         => $reconnect_every,
        read_timeout  => $read_timeout,
        write_timeout => $write_timeout
    );
    
    $self->logger->debug(sprintf("%s - Connecting to Redis", $worker));

    # Connect to Redis locally using a Unix socket
    my $unix_socket      = $self->config->get('/config/redis/@unix_socket')->[0];
    my $unix_socket_flag = $self->config->get('/config/redis/@use_unix_socket')->[0];

    # Handle deprecated flag
    if (defined($unix_socket_flag) && $unix_socket_flag eq '0') {
        $unix_socket = undef;
    }

    if (defined($unix_socket)) { 
      $redis_conf{sock} = $unix_socket;
      $self->logger->debug(sprintf("%s - Redis host unix socket:%s", $worker, $redis_conf{sock}));
    }
    # Connect to redis using an IP:Port configuration
    else {
      my $redis_host      = $self->config->get('/config/redis/@ip')->[0];
      my $redis_port      = $self->config->get('/config/redis/@port')->[0];
      $redis_conf{server} = "$redis_host:$redis_port";
      $self->logger->debug(sprintf("%s - Redis host %s:%s", $worker, $redis_host, $redis_port));
    }
    $self->logger->debug(sprintf("%s - Redis reconnect after %ss every %ss", $worker, $reconnect, $reconnect_every));
    $self->logger->debug(sprintf("%s - Redis timeouts [Read: %ss, Write: %ss]", $worker, $read_timeout, $write_timeout));

    my $redis;
    my $redis_connected = 0;

    # Try reconnecting to redis. Only continue when redis is connected.
    # Reconnection attepted every second
    while(!$redis_connected) {
        try {
            $redis = Redis::Fast->new( %redis_conf );
            $redis_connected = 1;
        }
        catch ($e) {
            $self->logger->error(sprintf("%s - Error connecting to Redis: %s", $worker, $e));
            sleep(1);
        };
    }

    return $redis;
}

sub _poll_cb {
    my $self   = shift;
    my %params = @_;

    # Set the arguments for the callback
    my $host    = $params{host};
    my $time    = $params{time};
    my $oid     = $params{oid};

    # Get params from the host object
    my $name         = $host->{name};
    my $ip           = $host->{ip};
    my $port         = $host->{port};
    my $context      = $host->{context};
    my $status       = $host->{status};
    my $vars         = $host->{host_variable};

    # Get some variables from the worker
    my $redis    = $self->redis;
    my $group    = $self->group_name;
    my $worker   = $self->worker_name;

    # Get a reference to the SNMP session
    my $snmp = $host->{snmp_session};

    # Get, find, or initialize the session's poll_id
    my $poll_id;
    my $poll_time;
    if (exists($status->{poll_id})) {
        $poll_id = $status->{poll_id};
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
            $self->logger->error(sprintf(
                "%s - Error fetching the current poll cycle ID from Redis for %s: %s",
                $worker,
                $name,
                $_
            ));
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
        $status->{poll_id} = $poll_id;
    }

    $self->logger->debug(sprintf("%s - _poll_cb processing %s:%s %s", $worker, $name, $port, $oid));

    # Reset the session's pending reply status for the OID
    # This advances pending replies even if we didn't get anything back for the OID
    # We do this because the polling cycle has completed in both cases
    delete $status->{pending_replies}{$oid};

    # Get the data returned by the session's SNMP request
    my $data = $snmp->var_bind_list;

    # Parse the values from the data for Redis as an array of "oid,value" strings
    my @values = map {sprintf("%s,%s", $_, $data->{$_})} keys(%{$data}) if ($data);

    # Handle errors where there was no value data for the OID 
    unless (@values) {
        my $error_data = {
            oid         => $oid,
            error       => 'OID_DATA_ERROR',
            description => $snmp->error(),
            timestamp   => time()
        };
        push(@{$status->{failed_oids}}, $error_data);
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

        # Always write defined value data to Redis
        $redis->select(0, sub{});
        $redis->sadd($host_keys{db0}, @values, sub{}) if (@values);

        # Write lookup key data to Redis
        # Only write if there are no pending replies left
        unless (keys(%{$status->{pending_replies}})) {

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

                $self->logger->debug("Adding host variables for $name");

                # Set Redis back to DB[0] to insert the var data with values
                $redis->select(0, sub{});

                # Add each host variable and its value (remove commas from var name)
                while(my ($var, $value) = each(%{$vars})) {
                    $var = ($var =~ s/,//gr);
                    $redis->sadd($host_keys{db0}, sprintf("vars.%s,%s", $var, $value->{value}), sub{});
                }

                # Set the host variable's interval lookup 
                $redis->select(3, sub{});
                $redis->hset($name_id, "vars", $group_interval, sub{});
                $redis->hset($ip_id,   "vars", $group_interval, sub{});
            }

            # Increment the poll_id
            $status->{poll_id}++;
        }

        # Set an OID lookup using vars and interval for the OIDs
        $redis->select(3, sub{});
        $redis->hset($name_id, $oid, $group_interval, sub{});
        $redis->hset($ip_id,   $oid, $group_interval, sub{});
    }
    catch ($e) {
        $self->logger->error(sprintf('%s - ERROR: could not hset Redis data: %s', $worker, $e));
    };

    # Ensure we're done with the pipeline
    $redis->wait_all_responses();

    # Update the global status for this host
    $self->_update_status($host);

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
        -port        => $host->{port},
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

    # Add each optional arg for the session if it's specified
    while (my ($arg, $attr) = each(%optional_args)) {
        $session_args{$arg} = $host->{$attr} if (exists($host->{$attr}) && defined($host->{$attr}));
    }

    return \%session_args;
}


=head2 _connect_to_snmp
    Uses host configs to generate SNMP args and create an SNMP session for every host.
    Stores the actual SNMP sessions in session hashes containing additional error and session info.
=cut
sub _connect_to_snmp {
    my $self  = shift;

    for my $host (@{$self->hosts}) {

        $self->logger->debug(sprintf(
            "%s - Creating SNMP session for %s:%s",
            $self->worker_name,
            $host->{name},
            $host->{port}
        ));

        # Get the SNMP version, host status, and hash of session args
        my $version   = $host->{snmp_version};
        my $status    = $host->{status};
        my $args      = $self->_get_session_args($host);

        # Report when there's no community specified and we have V1 or V2
        if ($version < 3 && !defined($args->{'-community'})) {
            my $error = sprintf("%s - SNMPv1 or v2 with no community set for %s!", $self->worker_name, $host->{name});
            my $error_data = {
                type        => 'community',
                description => $error,
                error       => $error,
                timestamp   => time()
            };
            push(@{$status->{snmp_errors}}, $error_data);
            $self->logger->error($error);
        }

        # Create the SNMP session
        my ($session, $session_error) = Net::SNMP->session(%{$args});

        # Keep the SNMP session object with the host config
        $host->{snmp_session} = $session;

        # Report errors that occurred while creating the SNMP session
        if (!$session || $session_error) {
            my $cid = exists($host->{context}) ? $host->{context} : 'N/A';
            my $error = sprintf(
                "%s - Error while creating SNMPv%s session for %s:%s (ContextID: %s): %s",
                $self->worker_name,
                $version,
                $host->{name},
                $host->{port},
                $cid,
                $session_error
            );
            $self->logger->error($error);

            my $error_data = {
                type        => 'session', 
                description => $error,
                error       => $error,
                timestamp   => time()
            };
            push(@{$status->{snmp_errors}}, $error_data);
        }
    }
}


sub _update_status {
    my $self = shift;
    my $host = shift;

    my $failed_oids = $host->{status}{failed_oids};
    my $snmp_errors = $host->{status}{snmp_errors};

    # Add the host config's errors to its global status data
    for my $error (@{$failed_oids}) {
        push(@{$self->status_data->{$host->{name}}{failed_oids}}, $error);
    }
    for my $error (@{$snmp_errors}) {
        push(@{$self->status_data->{$host->{name}}{snmp_errors}}, $error);
    }
}

sub _clear_status {
    my $self = shift;
    my $host = shift;

    $self->status_data->{$host->{name}} = {
        failed_oids => [],
        snmp_errors => []
    };
    $host->{status}{failed_oids} = [];
    $host->{status}{snmp_errors} = [];
    $self->logger->debug("Cleared status data for ".$host->{name});
}


sub _write_status_data {
    my $self = shift;

    # Don't write during the first cycle while status data is empty
    return if $self->first_run;

    # Aggregated hash of status data for each host
    my %status;

    # Aggregate status data by hostname
    while (my ($hostname, $status_data) = each(%{$self->status_data})) {

        # Init the host in the aggregated status data if needed
        unless (exists($status{$hostname})) {
            $status{$hostname} = {
                timestamp   => time(),
                failed_oids => {},
                snmp_errors => {
                    session   => [],
                    community => 0,
                    version   => 0
                },
                config      => $self->config->{config_file},
                interval    => $self->interval
            };
        }

        my $snmp_errors = $status_data->{snmp_errors};
        push(@{$status{$hostname}{snmp_errors}{session}}, @{$snmp_errors});
      
        my $failed_oids = $status_data->{failed_oids};
        for my $oid_err (@{$failed_oids}) {
            $status{$hostname}{failed_oids}{$oid_err->{oid}} = $oid_err;
        }
    }

    # Write the aggregated status data
    while (my ($hostname, $status_data) = each(%status)) {

        # Get the path of the host's status dir and status filename
        my $host_dir  = $self->status_dir . $hostname . '/';
        my $host_file = $host_dir . $self->group_name . '_status.json';
        $self->logger->debug("Writing status file: $host_file");

        # Check that the host's status dir exists and is accessible
        unless (-e $host_dir) {
            $self->logger->error("Could not find dir for monitoring data: $host_dir");
            return;
        }

        # Write out the status file
        if (open(my $fh, '>:encoding(UTF-8)', $host_file)) {
            print $fh JSON->new->pretty->encode($status_data);
            close($fh) || $self->logger->error("Could not close $host_file");
        }
        else {
            $self->logger->error("Could not open $host_file");
        }
    }
}


sub _collect_data {
    my $self = shift;

    my $hosts     = $self->hosts;
    my $oids      = $self->oids;
    my $timestamp = time;


    # Write status data for the previous polling cycle
    $self->_write_status_data();

    $self->logger->debug("----  START OF POLLING CYCLE FOR: \"" . $self->worker_name . "\"  ----");
    $self->logger->debug(sprintf(
        "%s - %s hosts and %s oids per host, max outstanding scaled to %s and queue is %s to %s",
        $self->worker_name,
        scalar(@{$hosts}),
        scalar(@{$oids}),
        $AnyEvent::SNMP::MAX_OUTSTANDING,
        $AnyEvent::SNMP::MIN_RECVQUEUE,
        $AnyEvent::SNMP::MAX_RECVQUEUE
    ));
    
    for my $host (@{$hosts}) {

        my $ip           = $host->{ip};
        my $name         = $host->{name};
        my $port         = $host->{port};
        my $context      = $host->{context};
        my $status       = $host->{status};
        my $snmp_session = $host->{snmp_session};

        # Used to stagger each request to a specific host, spread load.
        # This does mean you can't collect more OID bases than your interval
        # TODO: Deprecate this if non-impactful
        my $delay = 0;

        # Reset the host status before new requests are made
        $self->_clear_status($host);

        # Log error and skip collections if there are still pending replies
        if (keys(%{$status->{'pending_replies'}})) {
            $self->logger->error(sprintf(
                "%s - Unable to query device %s:%s in poll cycle, %s pending oid requests",
                $self->worker_name,
                $ip,
                $name,
                scalar(keys(%{$status->{pending_replies}}))
            ));
            for my $oid (keys(%{$status->{pending_replies}})) {
                my $error_data = {
                    oid         => $oid,
                    error       => 'OID_DATA_ERROR',
                    description => "No data returned for $oid within polling interval",
                    timestamp   => time() - $self->interval / 2,
                };
                push(@{$status->{failed_oids}}, $error_data);
            }
            next;
        }

        # Check the status of the SNMP session
        if (!defined($snmp_session)) {
            my $error = $self->worker_name." - No SNMP session defined for ".$name;
            my $error_data = {
                type        => 'session',
                description => $error,
                error       => $error,
                timestamp   => time()
            };
            push(@{$status->{snmp_errors}}, $error_data);
            $self->logger->error($error);

            # Skip when no SNMP session exists to prevent further failures
            next;
        }

        # Tracking request failures to compress logging
        my $failures = 0;

        # Initiate an SNMP request for each OID
        for my $oid_attr (@{$oids}) {

            my $oid = $oid_attr->{oid};

            # Determine which SNMPGET method we should use
            my $snmp_method = exists($oid_attr->{single}) ? "get_request" : "get_table";

            # Create a hash of arguments for the request
            my %args = (-delay => $delay++);
            if (exists($oid_attr->{single})) {
                $args{-varbindlist} = [$oid];
            }
            else {
                $args{-baseoid}        = $oid;
                $args{-maxrepetitions} = $self->request_size;
            }

            # Add the context to session args if present
            if (defined($context)) {
                $args{'-contextengineid'} = $context;
            }

            # Define the callback with params for the session args
            $args{'-callback'} = sub {
                $self->_poll_cb(
                    host   => $host,
                    time   => $timestamp,
                    oid    => $oid,
                );
            }; 

            # Use the SNMP session to request data using our args
            $self->logger->debug(sprintf(
                "%s - Issuing %s request for %s:%s -> %s",
                $self->worker_name,
                $snmp_method, 
                $name,
                $port,
                $oid
            ));

            my $res = $snmp_session->$snmp_method(%args);
            if ($res) {
                # Add the OID to pending replies if the request succeeded
                $status->{pending_replies}{$oid} = 1;
            }
            else {
                # Add oid and info to failed_oids if it failed during request
                my $error = sprintf(
                    "%s - %s request to %s:%s for %s failed: %s",
                    $self->worker_name,
                    $snmp_method,
                    $name,
                    $port,
                    $oid,
                    $snmp_session->error()
                );
                my $error_data = {
                    oid         => $oid,
                    error       => 'OID_REQUEST_ERROR',
                    description => $error,
                    timestamp   => time()
                };
                push(@{$status->{failed_oids}}, $error_data);
                $failures++;
            }
        }
        if ($failures) {
            my $error = sprintf(
                "%s - Requests to %s:%s failed for %s of %s OIDs",
                $self->worker_name,
                $name,
                $port,
                $failures,
                scalar(@{$oids}),
            );
            $self->logger->error($error);
        }
    }
    $self->logger->debug("----  END OF POLLING CYCLE FOR: \"" . $self->worker_name . "\"  ----");

    if ($self->first_run) {
        $self->_set_first_run(0);
    }
}

1;
