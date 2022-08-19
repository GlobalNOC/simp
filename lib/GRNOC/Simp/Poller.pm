package GRNOC::Simp::Poller;

use strict;
use warnings;

# Enables stack tracing when run with -d:Trace
$Devel::Trace::TRACE = 1;

use Moo;
use JSON;
use AnyEvent;
use Try::Tiny;
use Proc::Daemon;
use Data::Dumper;
use Devel::Size qw(size total_size);
use IPC::Shareable;
use File::Path 'remove_tree';
use POSIX qw( _exit setsid setuid setgid );
use Types::Standard qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Simp::Poller::Worker;

our $VERSION = '1.10.0';

### Required Attributes ###
=head2 public attributes
=over 12

=item pid_file
=item config_file
=item logging_file
=item hosts_dir
=item groups_dir
=item validation_dir
=item status_dir
=item daemonize
=item run_user
=item run_group

=back
=cut
has pid_file => (
    is       => 'ro',
    required => 1
);
has config_file => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has logging_file => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has hosts_dir => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has groups_dir => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has validation_dir => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has status_dir => (
    is       => 'rwp',
    isa      => Str,
    required => 1
);

### Optional Attributes ###
has daemonize => (
    is      => 'ro',
    isa     => Bool,
    default => 1
);
has run_user => (
    is       => 'ro',
    required => 0
);
has run_group => (
    is       => 'ro',
    required => 0
);

### Private Attributes ###
=head2 private_attributes
=over 12

=item pid
=item config
=item logger
=item event_loop
=item hosts
=item groups
=item workers
=item total_workers
=item obsolete_workers
=item running
=item reloading

=back
=cut

has pid            => (is => 'rwp');
has config         => (is => 'rwp');
has logger         => (is => 'rwp');
has event_loop     => (is => 'rwp');
has hosts          => (is => 'rwp');
has groups         => (is => 'rwp');
has workers        => (
    is      => 'rwp',
    default => sub {{}}
);
has total_workers => (
    is      => 'rwp',
    default => 0
);
has obsolete_workers => (
    is      => 'rwp',
    default => sub {[]}
);
has running => (
    is      => 'rwp',
    default => 1
);
has reloading => (
    is      => 'rwp',
    default => 0
);


=head2 BUILD
    Builds the simp_poller object and sets its parameters.
=cut
sub BUILD {
    my ($self) = @_;

    $self->_make_logger();
    $self->_make_config();
    $self->_make_status_dir();

    return $self;
}


=head2 start
    Starts all of the simp_poller processes
=cut
sub start {
    my ($self) = @_;

    # Clean up existing IPC shared memory configs for poller
    #IPC::Shareable->clean_up_all();

    # Daemonize
    if ($self->daemonize) {

        $self->logger->debug("Summoning simp-poller daemon");

        # Create a forked Daemon process
        # Resulting PID indicates the process we're in and status:
        # undef = Error forking
        #     0 = Running as daemon process
        #     N = Running as parent process
        my $pid = fork();
        $self->_error_exit("simp-poller daemon could not be summoned: $!") unless (defined($pid));

        # Exit the parent here so it doesn't execute any more code
        unless ($pid == 0) {
            $self->logger->info("simp-poller daemon has been summoned");
            _exit(0);
        }

        # Set the daemon as the session leader
        setsid() or $self->_error_exit("simp-poller daemon could not become session leader: $!");

        # Change the process name
        $0 = 'simp_poller [master]';

        # Create daemon PID file
        if ($self->daemonize) {
            if (open(my $fh, '>', $self->pid_file)) {
                print $fh $$;
                close($fh);
            }
            else {
                $self->_error_exit(sprintf("simp-poller daemon could not open PID file %s: %s", $self->pid_file, $!));
            }
        }

        # Redirect the STDOUT and STDERR from the controlling console to the daemon
        open(STDOUT, ">", "/dev/null") or $self->_error_exit("Could not redirect simp-poller daemon STDOUT: $!");
        open(STDERR, ">&", \*STDOUT) or $self->_error_exit("Cound not redirect simp-poller daemon STDERR: $!");
    }

    # Store the running PID
    $self->_set_pid($$);

    # Set the user and group of the process
    if (defined($self->run_group)) {
        my $gid = getgrnam($self->run_group);
        my $err = "Unable to set GID for group '%s': %s";
        $self->_error_exit(sprintf($err, $self->run_group, $!)) unless (defined($gid));
        $! = 0;
        setgid($gid);
        $self->_error_exit(sprintf($err, $self->run_group, $!)) if ($!);
    }
    if (defined($self->run_user)) {
        my $uid = getpwnam($self->run_user);
        my $err = "Unable to set UID for user '%s': %s";
        $self->_error_exit(sprintf($err, $self->run_user, $!)) unless (defined($uid));
        $! = 0;
        setuid($uid);
        $self->_error_exit(sprintf($err, $self->run_user, $!)) if ($!);
    }

    # Setup the signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info('Received SIG TERM.');
        $self->stop();
    };
    $SIG{'INT'} = sub {
        $self->logger->info('Recieved SIG INT.');
        $self->stop();
    };
    $SIG{'HUP'} = sub {
        $self->logger->info('Recieved SIG HUP.');
        $self->reload();
    };
    $self->logger->debug("Signal handlers ready");

    # Without a watcher, the event_loop AE condvar causes 100% CPU time on the daemon
    # Adding this watcher resolves the CPU time issue (outlined in AnyEvent::FAQ)
    # We don't use AE signal watchers because they cannot access $self
    # SIG CHLD is used and set to do nothing because it would otherwise be ignored
    my $anyevent_bugfix = AnyEvent->signal(signal => 'CHLD', cb => sub { (); });

    # Main event loop, runs until SIG TERM/INT/KILL
    while ($self->running) {

        $self->logger->info("Starting new event loop");
        $self->_set_event_loop(AnyEvent->condvar());

        $self->logger->info("Creating Redis configuration");
        $self->_make_redis_config();

        $self->logger->info("Creating host configurations");
        $self->_make_hosts_config();

        $self->logger->info("Creating group configurations");
        $self->_make_groups_config();
        
        $self->logger->info("Creating worker configurations");
        $self->_make_worker_config();

        $self->logger->info("Creating worker processes");
        $self->_make_workers();

        # Reload flag was set after receiving a SIG HUP
        if ($self->reloading) {
            
            # HUP all worker processes
            # Do this after workers are made to ensure workers are running
            my $pids = $self->_get_worker_pids();
            kill('HUP', @$pids);

            # Reset the reloading flag
            $self->_set_reloading(0);
        }

        $self->logger->info("Cleaning up obsolete worker processes");
        $self->_cleanup_workers();

        # Block the event loop until SIG is received
        $self->event_loop->recv;
        $self->logger->info("Ended the event loop");

    }
    
    # Remove PID file if daemonized
    unlink($self->pid_file) if ($self->daemonize);

    $self->logger->info("Poller exited");
    return 1;
}



=head2 reload
    Signal handling for SIGHUP.
    Reloads the main and all child processes
=cut
sub reload {
    my $self = shift;

    # Flag that we want to reload and signal to end the event loop
    $self->logger->info('Reloading');
    $self->_set_reloading(1);
    $self->event_loop->send;
}


=head2 stop
    Stops the simp_poller processes.
    If zombie PIDs are given, only kills those.
=cut
sub stop {
    my $self = shift;

    # Terminate all worker processes
    $self->logger->info('Stopping all worker processes');
    my $pids = $self->_get_worker_pids();
    kill('TERM', @$pids);

    # Flag that we should stop running and signal to end the event loop
    $self->_set_running(0);
    $self->event_loop->send;
}


=head2 _validate_config
    Will validate a config file given a file path, config object, and xsd file path
    Logs a debug message on success or logs and then exits on error
=cut
sub _validate_config {
    my $self   = shift;
    my $file   = shift;
    my $config = shift;
    my $xsd    = shift;

    # Validate the config
    my $validation_code = $config->validate($xsd);

    # Use the validation code to log the outcome and exit if any errors occur
    if ($validation_code == 1) {
        $self->logger->info("Validated $file");
        return 1;
    }
    else {
        if ($validation_code == 0) {
            $self->logger->error("ERROR: Failed to validate $file!");
            $self->logger->error($config->{error}->{error});
            $self->logger->error($config->{error}->{backtrace});
        }
        else {
            $self->logger->error("ERROR: XML schema in $xsd is invalid!\n" . $config->{error}->{backtrace});
        }
        exit(1);
    }
}

=head2 _make_shared_config
    Creates the variable in a shared memory segment as a JSON string.
=cut
sub _make_shared_config {
    my $self = shift;
    my $key  = shift;
    my $data = shift;

    # Add the process PID to the key to separate segments for multiple instances
    $key = sprintf("%s:%s", $key, $self->pid);

    # Valid Kibibyte sizes for IPC::Shareable segments
    my @sizes = (64, 128, 256, 512, 1024, 2048, 4096);    

    try {
        # JSONify the config data and calculate the raw and JSON data size
        my $data_size  = total_size($data) / 1000;
        my $data_json  = encode_json($data);
        my $json_size  = total_size($data_json) / 1000;
        $self->logger->debug(sprintf(
            "Compressed '%s' data from %s KiB to %s KiB of JSON",
            $key,
            $data_size,
            $json_size
        ));


        # Determine the appropriate shared memory size in kibibytes
        my $size;
        for (my $i = 0; $i <= $#sizes; $i++) {
            if ($json_size < $sizes[$i]) {
                $size = int(1024 * $sizes[$i]);
                last;
            }
        }
        $self->logger->info(sprintf(
            "Creating %sB shared memory cache for '%s' JSON (%sKiB data)",
            $size,
            $key,
            $json_size
        ));

        # Create the options to use for the shared memory segment
        my $share_options = {
            key       => $key,
            size      => $size,
            create    => 1,
            destroy   => 1,
            serialize => 'json'
        };

        # Tie a new scalar to shared memory and assign it to the JSON data
        tie (my $shared_json, 'IPC::Shareable', $share_options);
        $shared_json = $data_json;    
    } catch {
        $self->logger->error("Error creating shared memory json for '$key': $_");
    };
}


=head2 _make_logger
    Reads logging configs and makes a new logger
=cut
sub _make_logger {
    my $self = shift;

    # Create and store logger object
    my $grnoc_log = GRNOC::Log->new(config => $self->logging_file, watch => 120);
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger($logger);
    $self->logger->info("Set up the logger");
}


=head2 _make_config
    Reads and caches the primary config
=cut
sub _make_config {
    my $self = shift;

    # Create the main config object
    my $config = GRNOC::Config->new(
        config_file => $self->config_file,
        force_array => 1
    );

    # Get the validation file for config.xml
    my $xsd = $self->validation_dir . 'config.xsd';

    # Validate the main config useing the xsd file for it
    $self->_validate_config($self->config_file, $config, $xsd);

    # Set the config if it validated
    $self->_set_config($config);
    $self->logger->info("Set up the main configuration");
}


=head2 _make_status_dir
    Creates and sets up the directory needed for status file creation and writing
=cut
sub _make_status_dir {
    my $self = shift;

    # Set status_dir to path in the configs, if defined and is a dir
    my $status_path = $self->config->get('/config/status');
    my $status_dir  = $status_path ? $status_path->[0]->{dir} : undef;

    
    if (defined $status_dir && -d $status_dir) {
        $status_dir .= '.' if (substr($status_dir, -1) ne '/');
        $self->_set_status_dir($status_dir);
    }
    $self->logger->info("Set up status directories in ".$self->status_dir);
}


=head2 _make_redis_config
    Reads and caches the redis configuration for the main process and workers
=cut
sub _make_redis_config {
    my $self = shift;

    # Create the server from the host IP and port number
    my $redis_host = $self->config->get('/config/redis/@ip')->[0];
    my $redis_port = $self->config->get('/config/redis/@port')->[0];
    my $redis_server = $redis_host.':'.$redis_port;

    # Set the Redis config hash using keys outlined by Redis::Fast
    my %redis_config = (
        server          => $redis_server,
        sock            => $self->config->get('/config/redis/@unix_socket')->[0],
        reconnect       => $self->config->get('/config/redis/@reconnect')->[0],
        reconnect_every => $self->config->get('/config/redis/@reconnect_every')->[0],
        read_timeout    => $self->config->get('/config/redis/@read_timeout')->[0],
        write_timeout   => $self->config->get('/config/redis/@write_timeout')->[0]
    );
   
    $self->_make_shared_config('redis', \%redis_config);
    $self->logger->info("Set up the Redis configuration");
}


=head2 _get_config_objects
    Retrieves the XPath objects of a target from every XML file in a config dir.
    Returns the objects in a hash reference.
=cut
sub _get_config_objects {
    my $self       = shift;
    my $target_obj = shift;
    my $target_dir = shift;
    my $xsd        = shift;

    # The final hash of config objects to return
    my %config_objects;

    $self->logger->debug("Getting $target_obj XPath objects from $target_dir");

    # Load all files in the target_dir into an array
    opendir(my $dir, $target_dir);
    my @files = readdir $dir;
    closedir($dir);

    $self->logger->debug("Files found:\n" . Dumper(\@files));

    # Check every file in the target dir
    for my $file (@files) {

        # Only process valid XML files
        next unless $file =~ /^[^.#][^#]*\.xml$/;

        # Make an XPath object from the file
        my $config = GRNOC::Config->new(
            config_file => $target_dir . $file,
            force_array => 1
        );

        # Validate the config using the main config xsd file
        $self->_validate_config($file, $config, $xsd);

        # Push each targeted XPath object found in the file into the final array
        for my $object (@{$config->get($target_obj)}) {

            # Check if the object has been set to inactive
            unless ($object->{active} and $object->{active} == 0) {

                # Use the object name as the key to the object
                if (exists $object->{name}) {

                    # Check for a group key and for pre-existing configs; for merging multiple host configs.
                    if (exists $object->{group} && exists $config_objects{$object->{name}}) {
                       
                        # Merge in new group objects with existing ones 
                        $config_objects{$object->{name}}->{group} = {%{$config_objects{$object->{name}}->{group}}, %{$object->{group}}};
                    }
                    else {
                        $config_objects{$object->{name}} = $object;
                    }
                    delete $object->{name};
                }
                # Use the config file name as the key to the object
                else {
                    $config_objects{substr($file, 0, -4)} = $object;
                }
            }
            else {
                $self->logger->debug('Skipping inactive object');
            }
        }
    }

    $self->logger->debug("Got a total of " . scalar(keys %config_objects) . " $target_obj objects");
    return \%config_objects;
}


=head2 _make_hosts_config
    Process the host configs for their polling groups.
    Create and remove status dirs based upon which hosts are found.
    Set the hosts for the poller objects.
=cut
sub _make_hosts_config {
    my $self = shift;

    # Get the hosts from the hosts.d file
    my $hosts = $self->_get_config_objects(
        '/config/host', 
        $self->hosts_dir, 
        $self->validation_dir . 'hosts.xsd'
    );

    # Process each host in the config
    while (my ($host_name, $host_attr) = each(%$hosts)) {

        $host_attr->{name} = $host_name;

        # Parse the SNMP version (default is V2)
        my $snmp_version = ($host_attr->{snmp_version} =~ m/.*([123]).*/) ? $1 : '2';
        $host_attr->{snmp_version} = $snmp_version;

        $self->logger->error("Could not determine SNMP version for $host_name") unless ($snmp_version);

        # Infer transport domain from IP format; assume IPv4, otherwise IPv6. IP already regex validated by hosts.xsd in validation.d.
        $host_attr->{transport_domain} = $host_attr->{ip} =~ m/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/ ? 'udp4' : 'udp6';

        # Parse and set the ports for each polling group the host belongs to
        # Group ports will override host-wide ports
        # If no ports are specified, Net::SNMP will default to port 161
        my $ports = ['161'];
        $ports = $self->_parse_ports($host_attr->{ports}) if (exists($host_attr->{ports}));

        # Parse group-specific parameters for the host
        while (my ($group_name, $group_specific) = each(%{$host_attr->{group}})) {

            # Set any group-specific ports or use the default host ports
            if (exists($group_specific->{ports})) {
                $group_specific->{ports} = $self->_parse_ports($group_specific->{ports});
            }
            else {
                $group_specific->{ports} = $ports;
            }
    
            # Determine the load load it'll have on the worker
            # This will be N sessions per port and N*M sessions for port+context combos
            my $host_load = scalar(@$ports);

            # Get any context ID's for the group and factor in their effect on load
            if (exists($group_specific->{context_id})) {
                $host_load *= scalar(@{$group_specific->{context_id}});
            }
            
            # Set the host's load for the group
            $group_specific->{load} = $host_load;

            # Finally, initialize an error hash for tracking polling issues for the group+host
            $group_specific->{errors} = {};
        }

        # Check if status dir for each host exists, or create it.
        my $mon_dir = $self->status_dir . $host_name . '/';
        unless (-e $mon_dir || system("mkdir -m 0755 -p $mon_dir") == 0) {
            $self->logger->error("Could not find or create dir for monitoring data: $mon_dir: $!");
        }
        else {
            $self->logger->debug("Found or created status dir for $host_name successfully");
        }
    }

    # Once status dirs for configured hosts have been made...
    # Remove any dirs for hosts that are not included in any hosts.d file
    # Remove any status files in a valid host dir for groups
    # the host doesn't have configured
    for my $host_dir (glob($self->status_dir . '*')) {
        my $dir_name = (split(/\//, $host_dir))[-1];

        # Remove the dir unless the dir name exists in the hosts hash
        unless (exists $hosts->{$dir_name}) {
            $self->logger->debug("$host_dir was flagged for removal");
           
            my $rm_errors; 
            remove_tree([$host_dir], {error => $rm_errors});
            
            if ($rm_errors && @$rm_errors) {
                for my $err (@$rm_errors) {
                    my ($err_file, $err_msg) = %$err;
                    $err_file = $host_dir if ($err_file == '');
                    $self->logger->error("Failed to remove $err_file: $err_msg");
                }
            }
            else {
                $self->logger->debug("Successfully removed $host_dir");
            }
        }

        # When the dir exists, clean it up so only active groups are present
        else {
            for my $status_file (glob($host_dir . '/*')) {

                # Get the filename
                my $file = (split(/\//, $status_file))[-1];

                # Get the polling group's name
                $file =~ /^(.+)_status\.json$/;

                # Unless the host is configured for the group, delete the status file
                unless (exists $hosts->{$dir_name}{group}{$1}) {
                    if (unlink $status_file) {
                        $self->logger->debug("Removed status file $file in $host_dir");
                    }
                    else {
                        $self->logger->error("Failed to remove status file $file");
                    }
                }
            }
        }
    }

    $self->_set_hosts($hosts);
    $self->_make_shared_config('hosts', $hosts);
    $self->logger->info("Set up hosts configurations");
}


=head2 _make_groups_config
    Process the group configs for their polling oids.
    Set the groups for the poller object.
=cut
sub _make_groups_config {
    my $self = shift;

    # Get the default results-per-request size (max_repetitions)
    # from the poller config. (Default to 15)
    my $request_size = $self->config->get('/config/request_size');
    my $num_results = $request_size ? $request_size->[0]->{results} : 15;

    # Get the group objects from the files in groups.d
    my $group_objects = $self->_get_config_objects(
        '/group', 
        $self->groups_dir, 
        $self->validation_dir . 'group.xsd'
    );

    my %groups = %$group_objects;

    # Get the total number of workers to fork and
    # set the group name and any defaults
    my $total_workers = 0;

    while (my ($group_name, $group_attr) = each(%groups)) {

        # Set the oids for the group from the mib elements
        # Then, remove the mib attr to prevent redundant data under a confusing name
        $group_attr->{oids} = [];
        for my $oid (@{$group_attr->{mib}}) {
            push (@{$group_attr->{oids}}, $oid);
        }
        delete $group_attr->{mib};

        # Default the Redis retention time to 5x the polling interval
        $group_attr->{retention} = $group_attr->{interval} * 5 unless ($group_attr->{retention});

        # Default the results-per-packet max (max_repetitions)
        $group_attr->{request_size} = $num_results unless ($group_attr->{request_size});

        # Add the groups' workers to the total number of forks to create
        $total_workers += $group_attr->{workers};
    }

    $self->_set_groups(\%groups);
    $self->_set_total_workers($total_workers);
    $self->_make_shared_config('groups', \%groups);
    $self->logger->info('Set up polling group configurations');
}



=head2 _make_worker_config()
    Creates the hashes for each worker's configuration.
    These hashes are passed to each worker so they know what to collect.
=cut
sub _make_worker_config {
    my $self = shift;

    my %workers;

    # First, remove any groups currently in config that arent in the new config
    while (my ($group_name, $group_attr) = each(%{$self->groups})) {

        # Get the count of workers for the group
        my $worker_count = $group_attr->{workers};

        # Get the range of worker IDs as an array
        my @worker_ids = map {$_} (0 .. ($worker_count - 1));

        # Track the load each worker has been assigned
        my %worker_loads = map {$_ => 0} @worker_ids;

        # Initialize the data structure for the group's workers
        my %group_workers = map {$_, {hosts => [], pid => undef}} @worker_ids;
        $workers{$group_name} = \%group_workers;

        # Track the worker ID we should be adding a host to
        my $worker_id = 0;

        # Add hosts to workers and load balance them based upon the load of hosts the worker has
        while (my ($host_name, $host_attr) = each(%{$self->hosts})) {

            # Skip the host if it doesn't belong to the group
            next unless (exists($host_attr->{group}{$group_name}));

            my $host_load = $host_attr->{group}{$group_name}{load};

            # Add at least one host to each worker before load balancing
            if ($worker_loads{$worker_id} == 0) {

                # Add the host and it's load to our tracking hashes
                push(@{$workers{$group_name}{$worker_id}{hosts}}, $host_name);
                $worker_loads{$worker_id} += $host_load;

                # Increment the worker ID until each worker has a host, then set back to 0
                $worker_id = ($worker_id >= $#worker_ids) ? 0 : $worker_id + 1;

                # Skip to the next host
                next;
            }

            # Determine least-loaded worker if the current worker already has a load
            # Init the minimum load as the first worker's load
            my $min_load = ~0;

            # Check each worker's current load
            while (my ($id, $load) = each(%worker_loads)) {

                # Switch the worker ID and min load to the new worker if it has less load
                if ($min_load > $load) {
                    $min_load  = $load;
                    $worker_id = $id;
                }
            }

            # Add the worker and it's load to our tracking hashes
            push(@{$workers{$group_name}{$worker_id}{hosts}}, $host_name);
            $worker_loads{$worker_id} += $host_load;
        }

        # Remove any worker or group configs that don't have a host load
        my $has_load = 0;
        while (my ($id, $load) = each(%worker_loads)) {
            if ($load == 0) {
                $self->logger->debug("$group_name [$id] has no hosts and won't be started");
                delete $workers{$group_name}{$id};
            }
            else {
                $has_load++;
            }
        }
        unless ($has_load) {
            $self->logger->debug("Polling group '$group_name' removed from config, no hosts");
            delete $workers{$group_name};
        }
    }

    # Reconcile the new worker config with any existing worker config
    $self->_reconcile_workers(\%workers);

    # Set the new worker configs in local and shared memory
    $self->_set_workers(\%workers);
    $self->_make_shared_config('workers', \%workers);
    $self->logger->info("Set up worker configurations");
}


=head2 _reconcile_workers
    Compares new and old worker configs to reconcile their differences.
    Sets the existing PID for a worker in the new config if it's already running.
    Marks existing PIDs to clean up that no longer exist in the new config.
    If an entire group has been removed, all its workers will be stopped.
=cut
sub _reconcile_workers {
    my $self       = shift;
    my $new_config = shift;
    my $old_config = $self->workers;

    # Nothing to reconcile if no old config exists
    return unless(%$old_config);

    # Compare the old worker configs against the new ones
    while (my ($group_name, $old_workers) = each(%$old_config)) {
        
        # Validate that the group still exists in the new config
        my $valid_group = (exists($new_config->{$group_name})) ? 1 : 0;

        unless ($valid_group) {
            $self->logger->debug("$group_name removed in new configurations");
            next;
        }

        # Reconcile the individual workers
        while (my ($worker_id, $worker_attr) = each(%$old_workers)) {

            # Validate that the worker still exists in the new config
            my $valid_worker = (exists($new_config->{$group_name}{$worker_id})) ? 1 : 0;

            # Validate whether a valid worker has hosts
            my $valid_hosts = scalar($new_config->{$group_name}{$worker_id}{hosts});

            # Get any existing PID for the worker
            my $worker_pid = $worker_attr->{pid};

            # The group and/or worker does not exist in the new config
            if ((!$valid_group || !$valid_worker) || !$valid_hosts) {

                $self->logger->debug(sprintf(
                    "Worker %s [%s] with PID %s is obsolete, marking it for removal",
                    $group_name,
                    $worker_id,
                    $worker_pid
                ));

                # Add its PID to the array of obsolete worker processes to stop
                push(@{$self->obsolete_workers}, $worker_pid);
            }
            # The worker still exists in the new config and is currently running
            elsif (defined($worker_pid)) {

                # If the worker exists, does it have hosts?

                $self->logger->debug(sprintf(
                    "Found existing PID %s for %s [%s]",
                    $worker_pid,
                    $group_name,
                    $worker_id
                ));

                # Set the existing PID for the currently running worker
                $new_config->{$group_name}{$worker_id}{pid} = $worker_pid;
            }
            # The worker exists, but isn't running for some reason
            else {
                $self->logger->debug(sprintf(
                    "Something went wrong trying to reconcile configs for %s [%s]",
                    $group_name,
                    $worker_id
                ));
            }
        }
    }
}


=head2 _get_worker_pids
    Returns an array ref of all the running worker PIDs
=cut
sub _get_worker_pids {
    my $self = shift;
    my @pids;

    while (my ($group_name, $worker_ids) = each(%{$self->workers})) {
        while (my ($worker_id, $worker_attr) = each(%$worker_ids)) {
            my $pid = $worker_attr->{pid};
            next unless (defined($pid));
            push (@pids, $pid);
        }
    }
    return \@pids;
}


=head2 _cleanup_workers()
    Stops any running worker processes marked as obsolete.
    Processes are marked during _reconcile_workers()
=cut
sub _cleanup_workers {
    my $self = shift;
 
    my @pids = @{$self->obsolete_workers};

    if (scalar(@pids)) {

        $self->logger->debug("Killing obsolete workers. PIDs: ".join(', ',@pids));
  
        # Kill any workers marked obsolete, then empty the array of marked PIDs
        kill('TERM', @pids);
    }
    $self->_set_obsolete_workers([]);
}


=head2 _start_worker()
    This will create a new worker and start it.
    It takes a group name and worker ID along with the logger and status dir
    NOTE: This is NOT a method of $self because worker forks have no reference
=cut
sub _start_worker {
    my $parent_pid = shift;
    my $worker_id  = shift;
    my $group_name = shift;
    my $logger     = shift;
    my $status_dir = shift;

    my $worker = GRNOC::Simp::Poller::Worker->new(
        parent       => $parent_pid,
        id           => $worker_id,
        group        => $group_name,
        logger       => $logger,
        status_dir   => $status_dir,
    );
    $worker->start();    
}


=head2 _make_workers
    Creates the forked worker processes for each polling group.
    Delegates hosts to their polling groups' workers.
=cut
sub _make_workers {
    my ($self) = @_;

    # Create workers for each group
    while (my ($group_name, $group_workers) = each(%{$self->workers})) {

        $self->logger->info("Creating worker processes for $group_name");

        while (my ($worker_id, $worker_attr) = each(%{$self->workers->{$group_name}})) {

            # Ensure an existing worker process is still running
            if (defined($worker_attr->{pid})) {
                my $worker_running = kill(-0, $worker_attr->{pid});

                # Don't create a new process for the worker if it's running
                if ($worker_running) {
                    $self->logger->debug("$group_name [$worker_id] already running");
                    next;
                }
                # PID wasn't running, reset PID and restart the worker process
                else {
                    $self->logger->error("$group_name [$worker_id] should be running but isn't, restarting it");
                    $worker_attr->{pid} = undef;
                }
            }

            # Create a fork for new workers
            my $pid = fork();

            # PID is 0 if we're in the child process
            if ($pid == 0) {

                # Start a new worker in the child process, then POSIX exit back to the parent
                _start_worker($self->pid, $worker_id, $group_name, $self->logger, $self->status_dir);

                # Leaves the child process
                _exit(0);
            }
            # PID is the process number if we're in the parent again
            elsif (defined($pid)) {

                # Track the worker fork's PID inside the main process
                $self->workers->{$group_name}{$worker_id}{pid} = $pid;  
            }
            # PID is undef if a fork could not be created for some reason
            else {
                $self->logger->error("ERROR: Could not fork a child process for $group_name [$worker_id]");
            }
        }
    }
}


=head2 _parse_ports()
    Reads a CSV string containing ports and/or port ranges.
    Returns an arrayref of the port numbers from the string
=cut
sub _parse_ports {
    my $self     = shift;
    my $port_str = shift;

    # An array of port numbers to return
    my @ports;

    # Get the array of port specifications from CSV string
    my @csv_array = split(/ ?, ?/, $port_str);

    for (my $i = 0; $i <= $#csv_array; $i++) {

        my $entry = $csv_array[$i];

        # Parse out individual ports from a range
        if ($entry =~ m/\d+-\d+/) {

            # Split the range on the hyphen
            my @range = split(/-/, $entry);

            # Get the first and last port of the range
            my $first = $range[0];
            my $last  = $range[1];

            # Push a port number for every port in the range
            for (my $port = $first; $port le $last; $port++) {
                push(@ports, "$port");
            }
        }
        # Add single ports
        else {
            push(@ports, "$entry");
        }
    }
    return \@ports;
}


=head2 _error_exit()
    Does exactly what the name implies
=cut
sub _error_exit {
    my $self = shift;
    my $msg  = shift;

    $self->logger->error($msg);
    warn "$msg\n";
    _exit(1);
}


1;
