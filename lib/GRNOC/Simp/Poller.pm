package GRNOC::Simp::Poller;

use strict;
use warnings;

use lib '/opt/grnoc/venv/simp/lib/perl5';

use Moo 2.003000;
use Proc::Daemon 0.19;
use Data::Dumper 2.145;
use File::Path 2.09 'rmtree';
use Parallel::ForkManager 1.18;
use POSIX 1.30 qw( setuid setgid );
use Types::Standard 1.004002 qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Simp::Poller::Worker;

our $VERSION = '1.11.3';

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
=item group_hosts
=item total_workers
=item status_path
=item children
=item do_reload

=back
=cut

has config      => (is => 'rwp');
has logger      => (is => 'rwp');
has hosts       => (is => 'rwp');
has groups      => (is => 'rwp');
has group_hosts => (is => 'rwp');

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
    my $valid = $self->_validate_config($self->config_file, $config, $xsd);
    exit(1) unless ($valid);

    # Set the config if it validated
    $self->_set_config($config);
    $self->logger->debug("Finished configuring the main Poller process");

    # Set status_dir to path in the configs, if defined and is a dir
    my $status_path = $self->config->get('/config/status');
    my $status_dir = $status_path ? $status_path->[0]->{dir} : undef;
    if (defined $status_dir && -d $status_dir) {
        if (substr($status_dir, -1) ne '/') {
            $status_dir .= '/';
        }
        $self->_set_status_dir($status_dir);
    }
    $self->logger->debug("Set simp-poller status dir to " . $self->status_dir);

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
        $self->logger->debug("Validated $file");
        return 1;
    }
    elsif ($validation_code == 0) {
        $self->logger->error("ERROR: Failed to validate $file!");
        $self->logger->error($config->{error}->{error});
        $self->logger->error($config->{error}->{backtrace});
    }
    else {
        $self->logger->error("ERROR: XML schema in $xsd is invalid!\n" . $config->{error}->{backtrace});
    }
    return;
}


=head2 _get_config_objects
    Returns GRNOC::Config objects for every XML file in a dir.
    Validates each XML file before instantiating the config object.
=cut
sub _get_config_objects {
    my $self       = shift;
    my $config_dir = shift;
    my $config_xsd = shift;

    # Load all files in the target_dir into an array
    opendir(my $dir, $config_dir);
    my @files = readdir $dir;
    closedir($dir);
    $self->logger->debug("Config files found:\n" . Dumper(\@files));

    # Return this array of GRNOC::Config objects at the end
    my @configs;

    # Check every file in the target dir
    for my $file (@files) {

        # Only process valid XML files
        next unless $file =~ /^[^.#][^#]*\.xml$/;

        # Make an XPath object from the file
        my $config = GRNOC::Config->new(
            config_file => $config_dir . $file,
            force_array => 1
        );

        # Validate the config using the main config xsd file
        my $valid = $self->_validate_config($file, $config, $config_xsd);
        
        if ($valid) {
            push(@configs, {file => $file, config => $config});
        }
        else {
            $self->logger->error("ERROR: $file is invalid and will be ignored.");
        }
    }
    return \@configs;
}


=head2 _make_status_dir
    Initializes the status dir for a host if it does not exist.
=cut
sub _make_status_dir {
    my $self = shift;
    my $host = shift;
    my $host_dir = $self->status_dir . $host->{name};

    if (-e $host_dir) {
        $self->logger->debug("Found status dir: $host_dir");
    }
    elsif (mkdir($host_dir, oct('0744'))) {
        $self->logger->debug("Created status dir: $host_dir");
    }
    else {
        $self->logger->error("ERROR: Failed to create status dir: $host_dir");
    }
}


=head2
    Cleans up old host dirs and status files.
    This should be run after new host configs are processed.
=cut
sub _refresh_status_dirs {
    my $self  = shift;

    $self->logger->info("Refreshing simp-poller status files");

    # Once status dirs for configured hosts have been made...
    # Remove any dirs for hosts that are not included in a hosts.d file
    # Remove status files for groups a host doesn't have configured
    for my $path (glob($self->status_dir . '*')) {

        # Get the intended host name from the dir name
        my $host = (split(/\//, $path))[-1];

        # Validate if the host exists in any group
        my $valid = 0;
        for my $group_hosts (values(%{$self->group_hosts})) {
            if (exists($group_hosts->{$host})) {
                $valid = 1;
                last;
            }
        }
        
        # If the host was invalidated, remove its dir and skip to next host
        unless ($valid) {
            if (rmtree[$path]) {
                $self->logger->debug("Removed $path");
            }
            else {
                $self->logger->error("ERROR: Attempt to remove $path failed!");
            }
            next;
        }

        
        # If valid, clean up group files that are no longer configured
        for my $status_file (glob($path . '/*')) {

            # Get the filename
            my $group_file = (split(/\//, $status_file))[-1];

            # Get the polling group's name
            $group_file =~ /^(.+)_status\.json$/;

            # Unless the host is configured for the group, delete the status file
            unless (exists($self->group_hosts->{$1}{$host})) {
                unlink $status_file;
                $self->logger->debug("Removed $path/$group_file");
            }
        }
    }
}


=head2 _process_host_configs
    Process the host configs to use for polling groups.
    Create and remove hosts' status dirs based upon which hosts are found.
=cut
sub _process_host_configs {
    my $self = shift;

    $self->logger->debug(sprintf("Reading host configs in %s", $self->hosts_dir));

    # Hash of group names mapped to an array of their hosts' configs
    # Organized like: {group_name => [{host configuration}]}
    my %hosts;

    # Hash of group names mapped to a hash of host names for the group
    # Used for quickly checking which hosts are in which groups.
    my %group_hosts;

    # Tracking metrics for unique hosts and total polling configs
    my %host_metrics;

    # Get the valid GRNOC::Config objects from each hosts.d file
    my $hosts_dir     = $self->hosts_dir;
    my $hosts_xsd     = $self->validation_dir . 'hosts.xsd';
    my $hosts_objects = $self->_get_config_objects($hosts_dir, $hosts_xsd);

    # Process each hosts config object
    for my $hosts_object (@$hosts_objects) {

        my $file   = $hosts_object->{file};
        my $config = $hosts_object->{config};

        $self->logger->debug(sprintf("Processing hosts from %s", $file));

        # Process each host definition in the file
        for my $host (@{$config->get('/config/host')}) {

            # Create a status dir for the host
            # NOTE: This MUST occur before the host's "group" KVP is deleted
            $self->_make_status_dir($host);

            # Initialize a hash for tracking status data for this host config
            # It is used by the worker to track status and write monitoring files
            $host->{status} = {
                snmp_errors     => [],
                failed_oids     => [],
                pending_replies => {},
            };

            # SNMP version (default is V2)
            my $snmp_version = ($host->{snmp_version} =~ m/.*([123]).*/) ? $1 : '2';
            $host->{snmp_version} = $snmp_version;

            # Get transport domain from IP format.
            # Assume IPv4, otherwise IPv6.
            # IP already regex validated by XSD.
            my $transport = $host->{ip} =~ m/^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/ ? 'udp4' : 'udp6';
            $host->{transport_domain} = $transport;

            # Get the ports the host should use.
            # If the host has ports set for a group, those will be applied in the group loop.
            my $host_ports = $self->_parse_ports($host);

            # Copy the groups and group-level configs before deleting from the host
            # Group configs are applied to copies of the master host config hash.
            my $groups = {%{$host->{group}}};
            delete $host->{group};

            # Add the host config for each group
            while (my ($group_name, $group) = each(%$groups)) {

                # Initialize the group's host array if needed
                $hosts{$group_name} = [] unless (exists($hosts{$group_name}));

                # Initialize the group_hosts mapping if needed
                $group_hosts{$group_name} = {} unless (exists($group_hosts{$group_name}));

                # Track the hostname for the group
                $group_hosts{$group_name}{$host->{name}} = 1;
                
                # Get the ports used specifically for this group
                my $group_ports = $self->_parse_ports($group);

                # Determine whether to use host-defined or group-defined ports
                my $ports = ($group_ports) ? $group_ports : $host_ports;

                # Create a host config for each port that needs to be polled
                for my $port (@$ports) {                

                    # Copy the initial host config
                    my $host_copy = {%$host};

                    # Set the port and remove the array of ports in the copy
                    $host_copy->{port} = $port;
                    delete $host_copy->{ports};

                    # Check for SNMPv3 ContextEngineIDs for the group
                    # Duplicate the host config for each context
                    if (exists($group->{context_id})) {
                        for my $context (@{$group->{context_id}}) {
                            
                            # Create another copy of the host config hash
                            my $host_context_copy = {%$host_copy};            

                            # Add the context ID
                            $host_context_copy->{context} = $context;           
 
                            # Push the host config
                            push(@{$hosts{$group_name}}, $host_context_copy);            
                        }
                    }
                    # If no context IDs exist, push the host config
                    else {
                        push(@{$hosts{$group_name}}, $host_copy);
                    }
                }
            }
        }
        $self->logger->debug(sprintf("Finished processing hosts from %s", $file));
    }

    $self->_set_hosts(\%hosts);
    $self->_set_group_hosts(\%group_hosts);
    $self->logger->info("Finished processing all host configurations");
}


=head2 _process_groups_config
    Process the group configs for their polling oids.
    Set the groups for the poller object.
=cut
sub _process_groups_config {
    my $self = shift;

    # Final hash of group configs by name
    my %groups;

    $self->logger->debug(sprintf("Reading group configs in %s", $self->groups_dir));

    # Get the valid GRNOC::Config objects from each groups.d file
    my $groups_dir    = $self->groups_dir;
    my $groups_xsd    = $self->validation_dir . 'group.xsd';
    my $group_objects = $self->_get_config_objects($groups_dir, $groups_xsd);

    # Get the default results-per-request size (max_repetitions)
    # from the poller config. (Default to 15)
    my $request_size = $self->config->get('/config/request_size');
    my $num_results = $request_size ? $request_size->[0]->{results} : 15;

    # Get the total number of workers to fork and
    # set the group name and any defaults
    my $total_workers = 0;
    
    # Variables used to show configuration statistics 
    my $conf_info = "%s - Configuration: [workers: %s | hosts: %s | sessions: %s | oids: %s | interval: %ss | timeout: %ss | retention %ss]";
    my $load_info = "%s - Load: %s requests every %ss (%s average requests/sec)";
    my @conf_strs;
    my @load_strs;
    my $agg_stats = {
        groups   => 0,
        workers  => 0,
        hosts    => {},
        sessions => 0,
        load_avg => 0,
    };

    for my $group_object (@$group_objects) {   
 
        my $file  = $group_object->{file};
        my $group = $group_object->{config}->get('/group')->[0];

        # Parse the group name from the file name
        my $name = substr($file, 0, -4);

        # Ensure there are configured hosts for the group
        # Don't configure this group if it has no host configs
        unless (exists($self->hosts->{$name}) && scalar(@{$self->hosts->{$name}}) != 0) {
            $self->logger->info("No workers will be created for $name, it has no hosts");
            next;
        }
        else {
            $self->logger->debug("Processing group defined in $file");
        }

        # Set the oids for the group from the mib elements
        # Then, remove the mib attr to prevent redundant data under a confusing name
        $group->{oids} = [@{$group->{mib}}];
        delete $group->{mib};

        # Set default retention time to 5x the polling interval
        unless ($group->{retention}) {
            $group->{retention} = $group->{interval} * 5;
        }
        $self->logger->debug("Set retention period to $group->{retention}s");

        # Set the packet results size (max_repetitions) or default to size from poller config
        unless ($group->{request_size}) {
            $group->{request_size} = $num_results;
        }
        $self->logger->debug("Set request size to $group->{request_size} results");

        # Get the array of host configs for the group
        my $hosts = $self->hosts->{$name};
        for my $host (@$hosts) {
            $agg_stats->{hosts}{$host->{name}} = 1 unless exists($agg_stats->{hosts}{$host->{name}});
        }

        # Set total host and session counts needed
        $group->{unique_hosts} = scalar(keys(%{$self->group_hosts->{$name}}));
        $group->{snmp_sessions} = scalar(@{$self->hosts->{$name}});

        # Determine the load of the hosts configured for the group
        # Load represents the total requests per second of each worker for the group
        # Load = ((Hosts * OIDs) / Interval) / Workers
        $group->{load} = (scalar(@$hosts) * scalar(@{$group->{oids}})) / $group->{workers};

        # Add the groups' workers to the total number of forks to create
        $total_workers += $group->{workers};

        # Set the group config
        $groups{$name} = $group;

        # Record stats related to configuration load
        my $total_hosts   = scalar(@$hosts);
        my $total_oids    = scalar(@{$group->{oids}});
        my $total_workers = $group->{workers};
        my $worker_hosts  = $total_hosts / $total_workers;
        my $worker_reqs   = $worker_hosts * $total_oids;
        my $worker_avg    = $worker_reqs / $group->{interval};
        push(@conf_strs, sprintf(
            $conf_info,
            $name,
            $group->{workers},
            $group->{unique_hosts},
            $group->{snmp_sessions},
            scalar(@{$group->{oids}}),
            $group->{interval},
            $group->{timeout},
            $group->{request_size},
            $group->{retention},
        ));
        push(@load_strs, sprintf(
            $load_info,
            $name,
            $worker_reqs,
            $group->{interval},
            sprintf("%.2f", $worker_avg),
        ));
        $agg_stats->{groups} += 1;
        $agg_stats->{workers} += $total_workers;
        $agg_stats->{sessions} += $group->{snmp_sessions};
        $agg_stats->{load_avg} += $worker_avg;
    }

    # Log the configuration statistics in order
    for (my $i=0; $i < scalar(@conf_strs); $i++) {
        $self->logger->error(@conf_strs->[$i]);
        $self->logger->error(@load_strs->[$i]);
    }
    # Log the aggregated statistics for all configs
    $self->logger->error(sprintf(
        "Configuration Summary: [%s groups | %s workers | %s unique hosts | %s SNMP sessions | %s average requests/s]",
        $agg_stats->{groups},
        $agg_stats->{workers},
        scalar(keys(%{$agg_stats->{hosts}})),
        $agg_stats->{sessions},
        sprintf("%.2f", $agg_stats->{load_avg}),
    ));


    # Set the number of forks and then the groups for the poller object
    $self->_set_total_workers($total_workers);
    $self->_set_groups(\%groups);
    $self->logger->info('Finished processing all group configurations');
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
        $self->_process_host_configs();
        $self->_process_groups_config();
        $self->_refresh_status_dirs();
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
    my $self = shift;
    my $host = shift;

    my $port_str = $host->{ports};
    
    # Return only the default port if no definitions are found
    return ['161'] unless (exists $host->{ports});

    # An array of port numbers to return
    my @ports;

    # Get the array of port specifications from CSV string
    my @csv_array = split(/ ?, ?/, $host->{ports});

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

    $self->logger->info('Stopping child worker processes ' . join(' ', @pids) . '.');

    # Kill children, then wait for them to finish exiting
    my $res = kill('TERM', @pids);
}


=head2 _balance_workers
    Creates a configuration of hosts per worker given a group.
    Balances the polling load evenly between a group's workers.
=cut 
sub _balance_workers {
    my $self       = shift;
    my $group_name = shift;
    my $group      = shift;
    
    $self->logger->debug("Balancing host load across $group->{workers} $group_name workers");

    # Handle configs where workers > total hosts by reducing to the host count
    # This prevents workers from spawning with no hosts that die instantly
    my $host_count = scalar(@{$self->hosts->{$group_name}});
    if ($host_count < $group->{workers}) {
        $self->logger->error(sprintf(
            '%s has more workers than hosts, only %s of %s configured workers will be created',
            $group_name,
            $host_count,
            $group->{workers}
        ));
        $group->{workers} = $host_count;
        $self->total_workers -= ($group->{workers} - $host_count);
    }

    # Get the worker IDs, and track their load and assigned host configs
    my @workers = map {[]} (0 .. ($group->{workers} - 1));

    # Tracks the worker ID we should be adding a host to
    # and the worker ID all configs for a single hostname should belong to
    my $id = 0;

    # Map hostname to a worker ID/index
    # all configs of the same hostame+group are added to the same worker.
    my %host_map;

    # Assign hosts to workers while balancing the load of each worker
    for my $host (@{$self->hosts->{$group_name}}) {

        # Track the least-loaded worker's index
        # Init the minimum load as MAX_INT
        my $min_load = ~0;

        # Bypass load balancing for hosts that have already been added to a worker
        # Push them to the same worker instead
        if (exists($host_map{$host->{name}})) {
            push(@{$workers[$host_map{$host->{name}}]}, $host);
            next;
        }

        # Track the smallest load and assign the host to that worker
        # Minimum load initialized to MAX_INT
        my $min = ~0;
        for (my $i = 0; $i <= $#workers; $i++) {
            my $load = scalar(@{$workers[$i]});
            if ($min > $load) {
                $min = $load;
                $id = $i;
            }
        }

        # Add the host to the worker's host array
        push(@{$workers[$id]}, $host);
        
        # Track which worker the hostname belongs to
        $host_map{$host->{name}} = $id unless(exists($host_map{$host->{name}}));
    }
    return \@workers;
}

=head2 _create_workers
    Creates the forked worker processes for each polling group.
    Delegates hosts to their polling groups' workers.
=cut
sub _create_workers {
    my ($self) = @_;

    # Create a fork for each worker that is needed
    my $forker = Parallel::ForkManager->new($self->total_workers);

    while (my ($group_name, $group) = each(%{$self->groups})) {

        # Tells worker procs to log a message when they die
        $forker->run_on_finish(
            sub {
                my ($pid) = @_;
                $self->logger->error(sprintf("Worker with PID %s has died", $pid));
            }
        );

        # Get an array of load-balanced host arrays where index is the worker_id
        my $workers = $self->_balance_workers($group_name, $group);

        # Create workers using the load-balanced worker configuration
        for (my $worker_id = 0; $worker_id <= $#$workers; $worker_id++) {

            # Get the array of hosts for the worker ID
            my $hosts = $workers->[$worker_id];

            # Jump into the worker process
            my $pid = $forker->start();

            # PID == True for the parent process, but not the worker process
            if ($pid) {
                my $msg = sprintf(
                    "%s [%s] (PID %s) created for %s host configurations",
                    $group_name,
                    $worker_id,
                    $pid,
                    scalar(@$hosts)
                );
                $self->logger->info($msg);
                push(@{$self->children}, $pid);

                # Parent process continues loop without running the rest of its code
                next;
            }

            # Create the worker within the forked process
            my $worker = GRNOC::Simp::Poller::Worker->new(
                instance     => $worker_id,
                config       => $self->config,
                logger       => $self->logger,
                status_dir   => $self->status_dir,
                group_name   => $group_name,
                oids         => $group->{oids},
                interval     => $group->{interval},
                retention    => $group->{retention},
                request_size => $group->{request_size},
                timeout      => $group->{timeout},
                hosts        => $hosts || []
            );

            # Worker's start method should only return when it has stopped (TERM, No hosts, etc)
            $worker->start();

            # Exit the worker process if it has died
            $forker->finish();
        }
    }
    
    $self->logger->info('simp-poller startup complete');
    $forker->wait_all_children();
    $self->_set_children([]);
    $self->logger->info('All workers have died');
}

1;
