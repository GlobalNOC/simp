package GRNOC::Simp::Poller;

use strict;
use warnings;

use Moo;
use Proc::Daemon;
use Data::Dumper;
use File::Path 'rmtree';
use Parallel::ForkManager;
use POSIX qw( setuid setgid );
use Types::Standard qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Simp::Poller::Worker;

our $VERSION = '1.8.3';

### Required Attributes ###
=head2 public attributes
=over 12

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

=item config
=item logger
=item hosts
=item groups
=item total_workers
=item status_path
=item children
=item do_reload

=back
=cut

has config => (is => 'rwp');
has logger => (is => 'rwp');
has hosts  => (is => 'rwp');
has groups => (is => 'rwp');

has total_workers => (
    is      => 'rwp',
    default => 0
);
has children => (
    is      => 'rwp',
    default => sub {[]}
);
has do_reload => (
    is      => 'rwp',
    default => 0
);


=head2 BUILD
    Builds the simp_poller object and sets its parameters.
=cut
sub BUILD {
    my ($self) = @_;

    # Create and store logger object
    my $grnoc_log = GRNOC::Log->new(
        config => $self->logging_file,
        watch  => 120
    );
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger($logger);
    $self->logger->debug("Started the logger");

    # Create the main config object
    $self->logger->debug("Configuring the main Poller process");
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
    $self->logger->debug("Finished configuring the main Poller process");

    # Set status_dir to path in the configs, if defined and is a dir
    $self->logger->debug("Setting up status writing for Poller");
    my $status_path = $self->config->get('/config/status');
    my $status_dir = $status_path ? $status_path->[0]->{dir} : undef;

    if (defined $status_dir && -d $status_dir) {
        if (substr($status_dir, -1) ne '/') {
            $status_dir .= '/';
            $self->logger->debug("The path for status_dir didn't include a trailing slash, added it: $status_dir");
        }

        $self->_set_status_dir($status_dir);
        $self->logger->debug("Found poller_status dir in config, using: " . $self->status_dir);
    }
    else {
        # Use default if not defined in configs, or invalid
        $self->logger->debug("No valid poller_status dir defined in config, using: " . $self->status_dir);
    }
    $self->logger->debug("Finished setting up status writing for Poller");

    return $self;
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
        $self->logger->debug("Successfully validated $file");
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
        next unless $file =~ /\.xml$/;

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
                    $config_objects{$object->{name}} = $object;
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


=head2 _process_hosts_config
    Process the host configs for their polling groups.
    Create and remove status dirs based upon which hosts are found.
    Set the hosts for the poller objects.
=cut
sub _process_hosts_config {
    my $self = shift;

    # Get the hosts from the hosts.d file
    my $hosts = $self->_get_config_objects(
        '/config/host', 
        $self->hosts_dir, 
        $self->validation_dir . 'hosts.xsd'
    );

    # Process each host in the config
    while (my ($host_name, $host_attr) = each(%$hosts)) {

        # Parse the SNMP version (default is V2)
        my $snmp_version = ($host_attr->{snmp_version} =~ m/.*([123]).*/) ? $1 : '2';
        $host_attr->{snmp_version} = $snmp_version;

        $self->logger->error("Could not determine SNMP version for $host_name") unless ($snmp_version);

        # Parse and set the ports for each polling group the host belongs to
        # Group ports will override host-wide ports
        # If no ports are specified, Net::SNMP will default to port 161
        my $host_ports = $self->_parse_ports($host_attr->{ports}) if (exists($host_attr->{ports}));

        while (my ($group_name, $group_attr) = each(%{$host_attr->{group}})) {

            # Reference to the ports that should be used for the group
            my $ports;

            # Set ports using the group-specific ports
            if (exists($group_attr->{ports})) {
                $ports = $self->_parse_ports($group_attr->{ports});
            }
            # Set ports using the host-wide ports
            elsif ($host_ports) {
                $ports = $host_ports;
            }
            # Default to port 161 only
            else {
                $ports = ['161'];
            }
            $group_attr->{ports} = $ports; 

        }

        # Check if status dir for each host exists, or create it.
        my $mon_dir = $self->status_dir . $host_name . '/';
        unless (-e $mon_dir || system("mkdir -m 0755 -p $mon_dir") == 0) {
            $self->logger->error("Could not find or create dir for monitoring data: $mon_dir");
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

            # This needs constraints, but works as is
            unless (rmtree([$host_dir])) {
                $self->logger->error("Attempted to remove $host_dir, but failed!");
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
                    unlink $status_file;
                    $self->logger->debug("Removed status file $file in $host_dir");
                }
            }
        }
    }

    $self->_set_hosts($hosts);
    $self->logger->debug("Finished processing host configurations");
}


=head2 _process_groups_config
    Process the group configs for their polling oids.
    Set the groups for the poller object.
=cut
sub _process_groups_config {
    my $self = shift;

    # Get the default results-per-request size (max_repetitions)
    # from the poller config. (Default to 15)
    my $request_size = $self->config->get('/config/request_size');
    my $num_results = $request_size ? $request_size->[0]->{results} : 15;

    # Get the group objects from the files in groups.d
    my $groups = $self->_get_config_objects(
        '/group', 
        $self->groups_dir, 
        $self->validation_dir . 'group.xsd'
    );

    # Get the total number of workers to fork and
    # set the group name and any defaults
    my $total_workers = 0;

    while (my ($group_name, $group_attr) = each(%$groups)) {

        # Set the oids for the group from the mib elements
        # Then, remove the mib attr to prevent redundant data under a confusing name
        $group_attr->{oids} = [];
        for my $oid (@{$group_attr->{mib}}) {
            push (@{$group_attr->{oids}}, $oid);
        }
        delete $group_attr->{mib};

        $self->logger->debug("Optional settings for group $group_name");

        # Set retention time to 5x the polling interval
        unless ($group_attr->{retention}) {
            $group_attr->{retention} = $group_attr->{interval} * 5;
            $self->logger->debug("Retention Period: $group_attr->{retention} (default)");
        }
        else {
            $self->logger->debug("Retention Period: $group_attr->{retention} (specified)");
        }

        # Set the packet results size (max_repetitions) or default to size from poller config
        unless ($group_attr->{request_size}) {
            $group_attr->{request_size} = $num_results;
            $self->logger->debug("Request Size: $group_attr->{request_size} results (default)");
        }
        else {
            $self->logger->debug("Request Size: $group_attr->{request_size} results (specified)");
        }

        # Set the hosts that belong to the polling group
        $group_attr->{hosts} = ();
        while (my ($host_name, $host_attr) = each(%{$self->hosts})) {

            # Skip any hosts that don't belong to the group
            next unless (exists($host_attr->{group}{$group_name}));

            # Create a reference to any host-specific group parameters (ports, contexts)
            my $host_specific = $host_attr->{group}{$group_name};

            # Build a host object having the host's parameters for the group config
            my %host_obj = %$host_attr;
            
            # Put the hostname in the object
            $host_obj{name} = $host_name;

            # Get the ports needed for the polling group for this host
            $host_obj{ports} = $host_specific->{ports};

            # Initialize an error cache for the host
            $host_obj{errors} = {};

            # Add the count of sessions for the host to determine the load it has on the worker
            # This will be N sessions per port and N*M sessions for port+context combos
            my $host_load = scalar(@{$host_obj{ports}});

            # Get any context ID's for the group and factor in their effect on load
            if (exists($host_specific->{context_id})) {
                $host_obj{contexts} = $host_specific->{context_id};
                $host_load *= scalar(@{$host_obj{contexts}});
            }
        
            # Set the calculated load for this host
            $host_obj{load} = $host_load;

            # Remove the 'group' hash from the host object, the worker doesn't need it
            delete $host_obj{group};

            # Add the host object to the group's hosts array
            push(@{$group_attr->{hosts}}, \%host_obj);
        }

        # Add the groups' workers to the total number of forks to create
        $total_workers += $group_attr->{workers};
    }

    # Set the number of forks and then the groups for the poller object
    $self->_set_total_workers($total_workers);
    $self->_set_groups($groups);

    $self->logger->debug('Finished processing group configurations');
}

=head2 start
    Starts all of the simp_poller processes
=cut
sub start {
    my ($self) = @_;

    # Daemonized
    if ($self->daemonize) {

        $self->logger->debug('Starting Poller in daemonized mode');

        # Set the PID file from config or use the default
        my $pid_file = $self->config->get('/config/@pid-file')->[0] || '/var/run/simp_poller.pid';
        $self->logger->debug("PID FILE: " . $pid_file);

        # Create the main Poller daemon using our PID and initialize it
        my $daemon = Proc::Daemon->new(pid_file => $pid_file);
        my $pid    = $daemon->Init();

        # Jump out from the main Poller daemon process
        return if ($pid);

        $self->logger->debug('Created daemon process.');

        # Change the process name
        $0 = "simp_poller [master]";

        # Figure out what user/group (if any) to change to
        my $user_name  = $self->run_user;
        my $group_name = $self->run_group;

        # Set the process' group
        if (defined($group_name)) {
            my $gid = getgrnam($group_name);
            $self->_log_err_then_exit("Unable to get GID for group '$group_name'") unless (defined($gid));
            $! = 0;
            setgid($gid);
            $self->_log_err_then_exit("Unable to set GID to $gid ($group_name)") unless ($! == 0);
        }

        # Set the process' user
        if (defined($user_name)) {
            my $uid = getpwnam($user_name);
            $self->_log_err_then_exit("Unable to get UID for user '$user_name'") unless (defined($uid));
            $! = 0;
            setuid($uid);
            $self->_log_err_then_exit("Unable to set UID to $uid ($user_name)") unless ($! == 0);
        }
    }
    # Foreground
    else {
        $self->logger->info('Starting Poller in foreground mode');
    }

    # Setup the signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info('Received SIG TERM.');
        $self->stop();
    };
    $SIG{'HUP'} = sub {
        $self->logger->info('Received SIG HUP.');
        $self->_set_do_reload(1);
        $self->stop();
    # Create and store the host portion of the config
    };
    $self->logger->debug("Signal handlers ready");

    # Parse configs and create workers from within the reload loop
    # When reloaded, hosts.d and groups.d will be re-parsed
    while (1) {
        $self->logger->info("Processing host configurations");
        $self->_process_hosts_config();

        $self->logger->info("Processing group configurations");
        $self->_process_groups_config();

        $self->logger->info("Creating worker processes");
        $self->_create_workers();

        # We only arrive here if the loop is running or poller is killed
        $self->logger->info("Poller has exited successfully");

        last if (!$self->do_reload);
        $self->_set_do_reload(0);
    }

    return 1;
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


=head2 _log_err_then_exit()
    Does exactly what the name implies
=cut
sub _log_err_then_exit {
    my $self = shift;
    my $msg  = shift;

    $self->logger->error($msg);
    warn "$msg\n";
    exit 1;
}


=head2 stop
    Stops all of the simp_poller processes.
=cut
sub stop {
    my ($self) = @_;

    $self->logger->info('Stopping.');

    my @pids = @{$self->children};

    $self->logger->debug('Stopping child worker processes ' . join(' ', @pids) . '.');

    # Kill children, then wait for them to finish exiting
    my $res = kill('TERM', @pids);
}


=head2 _create_workers
    Creates the forked worker processes for each polling group.
    Delegates hosts to their polling groups' workers.
=cut
sub _create_workers {
    my ($self) = @_;

    # Create a fork for each worker that is needed
    my $forker = Parallel::ForkManager->new($self->total_workers);

    # Create workers for each group
    while (my ($group_name, $group_attr) = each(%{$self->groups})) {

        # Get the range of worker IDs in an array
        my @worker_ids = map {$_} (0 .. ($group_attr->{workers} - 1));

        # Track the session load a worker has been assigned
        my %worker_loads = map {$_ => 0} @worker_ids;

        # Track which hosts have been assigned to which worker
        my %worker_hosts = map {$_, []} @worker_ids;

        # Track the worker ID we should be adding a host to
        my $worker_id = 0;

        # Add hosts to workers and load balance them based upon the load of hosts the worker has
        for my $host_obj (@{$group_attr->{hosts}}) {            

            # Add at least one host to each worker before load balancing
            if ($worker_loads{$worker_id} == 0) {

                # Add the host and it's load to our tracking hashes
                push(@{$worker_hosts{$worker_id}}, $host_obj);
                $worker_loads{$worker_id} += $host_obj->{load};

                # Increment the worker ID until each worker has a host, then set back to 0
                $worker_id = ($worker_id >= $#worker_ids) ? 0 : $worker_id + 1;

                # Skip to the next host
                next;
            }

            # Determine least-loaded worker if the current worker already has a load
            # Init the minimum load as the first worker's load
            my $min_load = ~0 ;

            # Check each worker's current load
            while (my ($id, $load) = each(%worker_loads)) {

                # Switch the worker ID and min load to the new worker if it has less load
                if ($min_load > $load) {
                    $min_load  = $load;
                    $worker_id = $id;
                }
            }

            # Add the worker and it's load to our tracking hashes
            push(@{$worker_hosts{$worker_id}}, $host_obj);
            $worker_loads{$worker_id} += $host_obj->{load};
        }

        $self->logger->info("Creating $group_attr->{workers} worker processes for $group_name");

        # Keep track of children pids
        $forker->run_on_finish(
            sub {
                my ($pid) = @_;
                $self->logger->error(sprintf("%s worker with PID %s has died", $group_name, $pid));
            }
        );

        # Create workers
        for (my $worker_id = 0; $worker_id < $group_attr->{workers}; $worker_id++) {

            my $pid = $forker->start();

            # We're still in the parent if so
            if ($pid) {
                $self->logger->debug(sprintf("PID %s created for %s [%s]", $pid, $group_name, $worker_id));
                push(@{$self->children}, $pid);
                next;
            }

            # Create worker in this process
            my $worker = GRNOC::Simp::Poller::Worker->new(
                instance     => $worker_id,
                config       => $self->config,
                logger       => $self->logger,
                status_dir   => $self->status_dir,
                group_name   => $group_name,
                oids         => $group_attr->{oids},
                interval     => $group_attr->{interval},
                retention    => $group_attr->{retention},
                request_size => $group_attr->{request_size},
                timeout      => $group_attr->{timeout},
                hosts        => $worker_hosts{$worker_id} || {}
            );

            # This should only return when the worker has stopped (TERM, No hosts, etc)
            $worker->start();

            # Exit child process
            $forker->finish();
        }
    }

    $self->logger->info('Waiting for all child worker processes to exit.');

    $forker->wait_all_children();

    $self->_set_children([]);

    $self->logger->info('All child workers have exited.');
}

1;
