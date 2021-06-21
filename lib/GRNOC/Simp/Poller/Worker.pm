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
    # TODO: THIS WILL NEED TO FACTOR IN HOST SESSIONS USING THEIR LOADS, NOT JUST HOST COUNT
    if (scalar(@{$self->oids}) && scalar(@{$self->hosts})) {
        AnyEvent::SNMP::set_max_outstanding(scalar(@{$self->hosts}) * scalar(@{$self->oids}));
    }
    else {
        $self->logger->error(sprintf("%s - Hosts or OIDs were not defined!", $worker));
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
                write_timeout => $write_timeout );
    
    $self->logger->debug(sprintf("%s - Connecting to Redis", $worker));

    # unix socket
    my $use_unix_socket = $self->config->get('/config/redis/@use_unix_socket')->[0];
    if ( $use_unix_socket == 1 ) { 
      $redis_conf{sock} = $self->config->get('/config/redis/@unix_socket')->[0];
      $self->logger->debug(sprintf("%s - Redis Host unix socket:%s", $worker, $redis_conf{sock}));
    }else{
      my $redis_host      = $self->config->get('/config/redis/@ip')->[0];
      my $redis_port      = $self->config->get('/config/redis/@port')->[0];
      $redis_conf{server} = "$redis_host:$redis_port";
      $self->logger->debug(sprintf("%s - Redis Host %s:%s", $worker, $redis_host, $redis_port));
    }
    $self->logger->debug(sprintf("%s - Redis reconnect after %ss every %ss", $worker, $reconnect, $reconnect_every));
    $self->logger->debug(sprintf("%s - Redis timeouts [Read: %ss, Write: %ss]", $worker, $read_timeout, $write_timeout));

    my $redis;
    my $redis_connected = 0;

    # Try reconnecting to redis. Only continue when redis is connected.
    while(!$redis_connected) {
        try {
            # Try to connect twice per second for 30 seconds,
            # 60 attempts every 500ms.
            $redis = Redis::Fast->new( %redis_conf );
            $redis_connected = 1;
        }
        catch ($e) {
            $self->logger->error(sprintf("%s - Error connecting to Redis: %s. Trying Again...", $worker, $e));
        };
    }

    return $redis;
}

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
    my $group    = $self->group_name;
    my $worker   = $self->worker_name;

    # Get a reference to the SNMP session, context, and poll ID
    my $snmp    = $session->{session};
    my $port    = $session->{port};
    my $poll_id = $session->{poll_id};
    my $context = $session->{context};

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

        my $error = "%s - ERROR: %s failed to request %s: %s"; 
        $self->logger->error(sprintf($error, $worker, $name, $reqstr, $snmp->error()));
    }

    # Determine the expiration date of the data being stored in Redis
    my $expire = $time + $self->retention;

    # Special key for the interval of the group (used by DB[3] to declare host variables)
    my $group_interval = sprintf("%s,%s", $group, $self->interval);

    # Create a key from our hostname/ip, port, and context
    my $name_id = defined($context) ? "$name:$port:$context" : "$name:$port";
    my $ip_id   = defined($context) ? "$ip:$port:$context"   : "$ip:$port";

    # These hashes hold all of the keys for Redis for each DB entry
    my %host_keys = (
        db0 => sprintf("%s,%s,%s", $name_id, $group, $time),
        db1 => sprintf("%s,%s,%s", $name_id, $group,  $poll_id),
        db2 => sprintf("%s,%s",    $name_id, $group),
        db3 => sprintf("%s,%s",    $name_id, $poll_id)
    );
    my %ip_keys = (
        db0 => sprintf("%s,%s,%s", $ip_id, $group, $time),
        db1 => sprintf("%s,%s,%s", $ip_id, $group,  $poll_id),
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

	# Start transaction
	$redis->multi();

        $redis->select(0);

        # Assign our value data from SNMP polling
        $redis->sadd($host_keys{db0}, @values) if (@values);

        # Check that the session has no pending replies left
        if (scalar(keys(%{$session->{'pending_replies'}})) == 0) {

            # Set the expiration time for the master key once all replies are received
            $redis->expireat($host_keys{db0}, $expire);

            # Create keys for the Host, IP and Time
            my $time_key = sprintf("%s,%s", $poll_id, $time);

            # Set the Poll ID => Timestamp lookup table data and its expiration
            $redis->select(1);
            $redis->set(     $host_keys{db1}, $host_keys{db0});
            $redis->set(       $ip_keys{db1}, $ip_keys{db0});
            $redis->expireat($host_keys{db1}, $expire);
            $redis->expireat(  $ip_keys{db1}, $expire);

            # Set the Host/IP => Timestamp lookup table data
            $redis->select(2);
            $redis->set(     $host_keys{db2}, $time_key);
            $redis->set(       $ip_keys{db2}, $time_key);
            $redis->expireat($host_keys{db2}, $expire);
            $redis->expireat(  $ip_keys{db2}, $expire);

            # Add any host-defined variables to the SNMP data in Redis as "vars.$var,$value"
            if (defined($vars)) {

                $self->logger->debug("Adding host variables for $name");

                # Set Redis back to DB[0] to insert the var data with values
                $redis->select(0);

                # Add each host variable and its value (remove commas from var name)
                while(my ($var, $value) = each(%$vars)) {
                    $var = ($var =~ s/,//gr);
                    $redis->sadd($host_keys{db0}, sprintf("vars.%s,%s", $var, $value->{value}));
                }

                # Set the host variable's interval lookup 
                $redis->select(3);
                $redis->hset($name_id, "vars", $group_interval);
                $redis->hset($ip_id,   "vars", $group_interval);
            }

            # Increment the session's poll_id
            $session->{poll_id}++;
        }

        # Set an OID lookup using vars and interval for the OIDs
        $redis->select(3);
        $redis->hset($name_id, $oid, $group_interval);
        $redis->hset($ip_id,   $oid, $group_interval);
    }
    catch ($e) {
        $self->logger->error(sprintf('%s - ERROR: could not hset Redis data: %s', $worker, $e));
    };

    # Execute our transaction
    my $redis_response = $redis->exec();

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
	-timeout     => $self->timeout
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
        $host_attr->{sessions} = [];

        # Get the hostname, SNMP version, array of session args, and failed session tracker
        my $host_name    = $host_attr->{name};
        my $context_ids  = $host_attr->{contexts};
        my $version      = $host_attr->{snmp_version};
        my $session_args = $self->_get_session_args($host_attr);
        my $failed       = 0;

        # Get the current poll ID for the host from Redis, stored by each session
        # Initializes/Defaults to 0
        my $poll_id;
        try {
            $self->redis->wait_all_responses();
            $self->redis->select(2);
            $poll_id = $self->redis->get($host_name . ",main_oid");
        }
        catch ($e) {
            $self->logger->error("Error fetching the current poll cycle ID from Redis for $host_name: $e");
            $self->redis->wait_all_responses();
            $self->redis->select(0);
        };

        # Create the session data hashes for each session
        # Apply the session args that were generated, multiplying by context IDs
        # If we don't have any contexts, pretend we do to avoid duplicate code
        for my $args (@$session_args) {

            $self->logger->debug(sprintf("%s - Creating session for %s:%s", $self->worker_name, $host_name, $args->{'-port'}));

            # This will allow the next loop to run when there are not context IDs
            my $total_contexts = exists($host_attr->{contexts}) ? scalar(@{$host_attr->{contexts}}) : 1;

            for (my $i = 0; $i < $total_contexts; $i++ ) {

                my $context_id = $context_ids->[$i];

                # We store the SNMP session, poll_id, port, context, and error data in this hash
                my %session_data = (
                    session         => 0,
                    poll_id         => $poll_id || 0,
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
                $session_data{session} = $snmp_session;
            
                # Add the session hash to the host's sessions array
                push(@{$host_attr->{sessions}}, \%session_data);

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
    
    $self->logger->debug("Writing status file $mon_dir");

    my %mon_data = (
        timestamp   => time(),
        failed_oids => '',
        snmp_errors => '',
        config      => $self->config->{config_file},
        interval    => $self->interval
    );

    # Aggregation of all the error data for each session's failed OIDs
    my $failed_oids = {};

    # Aggregation of each session's SNMP errors
    # Note: The community and version subsections are deprecated by config validation
    # They are left in here for backward compatibility with poller-monitoring.
    my $snmp_errors = {
        session   => [],
        community => 0,
        version   => 0
    };
    my $session_errors = $snmp_errors->{session};

    for my $session (@{$host->{sessions}}) {
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
                    Dumper($session->{pending_replies})
                ));
                next;
            }

            # Get the open SNMP session object
            my $snmp_session = $session->{session};

            if (!defined($snmp_session)) {
                $self->logger->error($self->worker_name . " - No SNMP session defined for $host_name");
                next;
            }

            for my $oid_attr (@$oids) {

                my $oid     = $oid_attr->{oid};
                my $is_leaf = $oid_attr->{single} || 0;

                # Log an error if the OID is failing
                if (exists $session->{'failed_oids'}{$oid}) {
                    $self->logger->error($session->{'failed_oids'}{$oid}{'error'});
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
                $self->logger->debug(sprintf("%s - %s request for %s", $self->worker_name, $get_method, $reqstr));
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
                    $self->logger->error(sprintf("%s - %s: %s", $self->worker_name, $res_err, $snmp_session->error()));
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
