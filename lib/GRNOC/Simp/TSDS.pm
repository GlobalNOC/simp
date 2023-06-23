package GRNOC::Simp::TSDS;

our $VERSION = '1.11.2';

# Documentation commands in Perl Pod format.
# see: https://perldoc.perlorg/perlpod.

=head1 GRNOC::SIMP::TSDS
    The main module providing a Simp to TSDS interface.
    Simp::TSDS requests data collections from GRNOC::Simp::Comp via AMQP.
    GRNOC::Simp::TSDS::Worker procs initiate requests unless run with --nofork.
    Specific collections are set via collection.xml configurations.
=cut

use strict;
use warnings;

use Moo;
use AnyEvent;
use Try::Tiny;
use Data::Dumper;
use File::Basename;
use JSON::XS qw(decode_json);
use POSIX qw(setuid setgid);
use Proc::Daemon;
use Parallel::ForkManager;

use GRNOC::Log;
use GRNOC::Config;
use GRNOC::RabbitMQ::Client;
use GRNOC::Simp::TSDS::Worker;
use GRNOC::Monitoring::Service::Status;

$Data::Dumper::Indent = 2;

=head2 PUBLIC ATTRIBUTES
=over 4
=item config_file
=item logging_file
=item pid_file
=item collections_dir
=item daemonize
=item run_user
=item run_group
=item status_dir
=back
=cut
has config_file     => (is => 'ro', required => 1);
has logging_file    => (is => 'ro', required => 1);
has pid_file        => (is => 'ro', required => 1);
has collections_dir => (is => 'ro', required => 1);
has validation_dir  => (is => 'ro', required => 1);
has daemonize       => (is => 'ro', required => 1);
has run_user        => (is => 'ro', required => 1);
has run_group       => (is => 'ro', required => 1);
has status_dir      => (is => 'ro', required => 1);


=head2 PRIVATE ATTRIBUTES
=over 4
=item config
=item logger
=item rmq_config
=item tsds_config
=item health_checker
=item worker_dir
=item collections
=item workers
=item total_workers
=item stagger
=item do_reload
=back
=cut
has config         => (is => 'rwp');
has logger         => (is => 'rwp');
has rmq_config     => (is => 'rwp');
has tsds_config    => (is => 'rwp');
has health_checker => (is => 'rwp');
has worker_dir     => (is => 'rwp');
has collections    => (is => 'rwp', default => sub { {} });
has workers        => (is => 'rwp', default => sub { {} });
has total_workers  => (is => 'rwp', default => 0);
has stagger        => (is => 'rwp', default => 1);
has do_reload      => (is => 'rwp', default => 0);


=head2 PRIVATE METHODS
=item BUILD
    Initialization method for the Moo Simp::TSDS class.
    This runs when GRNOC::Simp::TSDS->new() is called.
=cut
sub BUILD {
    my $self = shift;

    # Create and store the logger.
    my $grnoc_log = GRNOC::Log->new(
        config => $self->logging_file,
        watch  => 120
    );
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger($logger);
    $self->logger->debug("Started the logger");

    # Set the workers' status dir from the main status dir
    $self->_set_worker_dir($self->status_dir . 'worker/');

    return $self;
}

=item _validate_config()
    Will validate a config file given a file path, config object, and xsd file path
    Logs a debug message on success or logs and then exits on error
=cut
sub _validate_config {
    my $self   = shift;
    my $file   = shift;
    my $config = shift;
    my $xsd    = shift;

    # Validate the config.
    my $validation_code = $config->validate($xsd);

    # Use the validation code to log the outcome and exit if any errors occur.
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

=item _process_config()
    Loads and processes the main Simp::TSDS configuration.
    Sets the RabbitMQ, TSDS, config, and stagger private attributes.
=cut
sub _process_config {
    my ($self) = @_;

    $self->logger->info("Processing configuration from " . $self->config_file);

    # Create the main config object.
    $self->logger->debug("Configuring the main Simp::TSDS process");
    my $config = GRNOC::Config->new(
        config_file => $self->config_file,
        force_array => 1
    );

    # Get the validation file for configxml.
    my $xsd = $self->validation_dir . 'config.xsd';

    # Validate the main config useing the xsd file for it.
    my $valid = $self->_validate_config($self->config_file, $config, $xsd);
    exit(1) unless ($valid);

    # Set the main config object and contained parameters.
    $self->_set_config($config);
    $self->_set_rmq_config($config->get('/config/rabbitmq')->[0]);
    $self->_set_tsds_config($config->get('/config/tsds')->[0]);
    $self->_set_stagger($config->get('/config/stagger/@seconds')->[0]);

    $self->logger->debug("Finished configuring the main Simp::TSDS process");
}

=item _process_collections()
    Loads and processes the collections.d configurations.
    These are used in the creation of Simp::TSDS::Worker procs.
    Also sets the max forked procs required by collections.
=cut
sub _process_collections {
    my ($self) = @_;

    $self->logger->debug('Processing collection configurations from '.$self->collections_dir);

    # Open the collections.d directory.
    my $err;
    opendir my $dir, $self->collections_dir or $err = 1;
    $self->logger->error(sprintf("Could not open %s: %s", $self->collections_dir, $!)) if $err;

    # Get all XML file names found in the directory.
    my @config_files = grep { $_ =~ /^[^.#][^#]*\.xml$/ } readdir $dir;

    # Close the collections.d directory.
    closedir $dir or $err = 1;
    $self->logger->error(sprintf("Could not close %s: %s", $self->collections_dir, $!)) if $err;

    # Get the XSD file for validating collection configs.
    my $xsd = sprintf("%s%s", $self->validation_dir, "collection.xsd");

    # Create a hash of collection configs with compound keys of their unique params.
    # If the compound key doesn't match others for the composite, it is unique.
    # Unique collection configs are guaranteed their own worker processes.
    my %collections;

    # Count the total workers for all collections
    # This count is used by Parallel::ForkManager
    my $total_workers = 0;

    for my $file (@config_files) {

        # Create a new config object for the file.
        my $config = GRNOC::Config->new(
            config_file => sprintf("%s%s", $self->collections_dir, $file),
            force_array => 1
        );

        # Validate the collection config and skip if invalid.
        next unless ($self->_validate_config($file, $config, $xsd));

        # Process each collection in the config and push the result to collections.
        for my $collection (@{$config->get('/config/collection')}) {

            # Get the collection's config parameters.
            # Default the hosts to an empty array if not found.
            my $composite   = $collection->{'composite'};
            my $workers     = $collection->{'workers'};
            my $required    = $collection->{'required_values'} || "";
            my $measurement = $collection->{'measurement_type'};
            my $interval    = $collection->{'interval'};
            my $hosts       = $collection->{'host'} || [];
            my $exclusions  = $collection->{'exclude'} || [];
            my $filter      = {
                'name'  => $collection->{'filter_name'},
                'value' => $collection->{'filter_value'}
            };

            # Add the number of worker child procs needed for this collection.
            $total_workers += $workers;

            # Set the required values to an array using the CSV string
            $required = [split(',', $required)];

            # Set the array of host names to an array of hostname and push parameter hashes.
            # Hosts must push to the right measurement and check required values.
            $hosts = [map {{'name' => $_, 'measurement' => $measurement, 'required' => $required}} @$hosts];

            # Create the array of any exclusion rule strings
            $exclusions = [grep { defined($_->{'var'}) && defined($_->{'pattern'}) && (length($_->{'var'}) > 0) } @$exclusions];
            $exclusions = [map { "$_->{'var'}=$_->{'pattern'}" } @$exclusions];

            # Create the compound key for the collection config
            # Worker groups must be unique to their composite and interval.
            # The key is used in the collection's worker process names.
            my $key = sprintf("%s (%ss)", $composite, $interval);

            # Add workers, hosts, and exclusions to existing collections with the same composite+interval.
            if (exists($collections{$key})) {
                my $existing = $collections{$key};
                $existing->{'workers'} += $collection->{'workers'};
                push(@{$existing->{'hosts'}}, @$hosts);
                push(@{$existing->{'exclusions'}}, @$exclusions);
            }
            # Initialize the collection type for an interval and composite when first seen.
            else {
                $collections{$key} = {
                    'name'       => $key,
                    'composite'  => $composite,
                    'interval'   => $interval,
                    'workers'    => $workers,
                    'hosts'      => $hosts,
                    'exclusions' => $exclusions,
                    'filter'     => $filter
                };
            }
        }
    }
    $self->_set_total_workers($total_workers);
    $self->_set_collections(\%collections);
}


=head2 _balance_workers
    Creates an array of host config assignments per worker given a collection.
    The array index is the worker ID and its element is the array of configs.
    This can't be run until all collection configs have been processed.
=cut 
sub _balance_workers {
    my ($self, $collection) = @_;
    
    $self->logger->debug(sprintf(
        "Balancing host load across %s %s workers",
        $collection->{'workers'},
        $collection->{'name'},
    ));

    # Handle collections having a total worker count of zero
    if ($collection->{'workers'} < 1) {
        $self->logger->error(sprintf("Zero worker procs set for %s", $collection->{'name'}));
        return [];
    }

    # Handle collections where workers > total hosts by reducing the worker count.
    # This prevents workers from spawning with no hosts that die instantly.
    my $max_workers = scalar(@{$collection->{'hosts'}});
    if ($max_workers < $collection->{'workers'}) {
        $self->logger->error(sprintf(
            '%s has more workers than hosts, only %s of %s configured workers will be created',
            $collection->{'name'},
            $max_workers,
            $collection->{'workers'}
        ));
        $collection->{'workers'} = $max_workers;
        $self->total_workers -= ($collection->{'workers'} - $max_workers);
    }

    # Get the worker IDs and arrays for their assigned host configs
    my @workers = map {[]} (0 .. ($collection->{'workers'} - 1));

    # Track the worker ID getting host assignments
    my $id = 0;

    # Assign hosts to workers evenly
    for my $host (@{$collection->{'hosts'}}) {

        # Add the host to the worker's host array
        push(@{$workers[$id]}, $host);

        # Go to the next worker or reset to zero
        $id++;
        $id = 0 if ($id > $#workers);
       
    }
    return \@workers;
}


=item _create_workers()
    Creates and starts all collection workers.
    When a TERM, INT, or HUP is received, the workers are told to quit once.
    Blocks until all worker child procs have been reaped.
=cut
sub _create_workers {
    my $self = shift;

    # Create a total number of forks for each worker that is needed
    my $forker = Parallel::ForkManager->new($self->total_workers);

    # Callback for worker procs to run when reaped
    $forker->run_on_finish(sub {
        my ($pid) = @_;
        $self->logger->error(sprintf("Worker with PID %s has died", $pid));   
    });

    # Track the number of $stagger seconds off the interval for each worker
    my $offset = 0;

    # Spawn workers for each collection
    for my $collection (values(%{$self->{'collections'}})) {

        # Allocate hosts evenly to each worker
        my $workers = $self->_balance_workers($collection);

        # Create workers using the balanced host arrays
        for (my $id = 0; $id <= $#$workers; $id++) {

            # Get the array of hosts for the worker ID
            my $hosts = $workers->[$id];

            # Calculate the stagger amount for the worker between 0 and half the interval.
            # Prevent a worker from staggering past mid-interval + the actual stagger.
            # Offset is increased for the next worker's calculation after $stagger is calculated.
            # (e.g. $stagger = 65s and $interval = 60s: Modulo interval changes it to 5s.)
            my $stagger = ($offset++ * $self->stagger) % ($collection->{'interval'} / 2);

            # Jump into the worker process
            my $pid = $forker->start();

            # PID == True for the parent process, but not the worker process
            if ($pid) {
                $self->logger->info(sprintf(
                    "simp_tsds(%s) [%s] (PID %s) created for %s host configurations",
                    $collection->{'name'},
                    $id,
                    $pid,
                    scalar(@$hosts)
                ));

                # Set the worker PID to the collection config in the worker cache
                $self->workers->{$pid} = $collection;

                # Parent process continues the loop without running the rest of the code
                next;
            }

            # Create the worker within the forked process
            my $worker = GRNOC::Simp::TSDS::Worker->new(
                name             => $collection->{'name'},
                id               => $id,
                hosts            => $hosts,
                composite        => $collection->{'composite'},
                measurement      => $collection->{'measurement'},
                interval         => $collection->{'interval'},
                exclusions       => $collection->{'exclusions'},
                filter           => $collection->{'filter'},
                logger           => $self->logger,                
                rmq_config       => $self->rmq_config,
                tsds_config      => $self->tsds_config,
                status_dir       => $self->worker_dir,
                stagger          => $stagger
            );

            # Worker's start method should only return when it has stopped (TERM, No hosts, etc)
            $worker->start();

            # Exit the worker process if it has died
            $forker->finish();
        }
    }

    # Create a watcher that checks worker health on an interval.
    # NOTE: The 15s interval is a hard minimum for any simp-tsds collection interval.
    $self->_set_health_checker(AnyEvent->timer(
        after       => 15,
        interval    => 15,
        cb          => sub { $self->_check_worker_health(); }
    ));

    $self->logger->info('simp-tsds startup complete');

    # This blocks until all of forker's child procs are reaped.
    $forker->wait_all_children();
    $self->_set_workers({});
    $self->logger->info('All simp-tsds workers have died');
}

=item _make_status_dir()
    Initializes the status dir for a host if it does not exist.
=cut
sub _make_status_dir {
    my $self = shift;
    my $worker_dir = $self->status_dir . "workers/";

    if (-e $worker_dir) {
        $self->logger->debug("Found status dir: $worker_dir");
    }
    elsif (mkdir($worker_dir, oct('0744'))) {
        $self->logger->debug("Created status dir: $worker_dir");
    }
    else {
        $self->logger->error("ERROR: Failed to create status dir: $worker_dir");
    }
}

=item _delete_status_files()
    Cleans up old status files.
    This should run after new configs are processed.
=cut
sub _delete_status_files {
    my $self  = shift;

    $self->logger->info("Deleting old simp-tsds status files");

    # Create an array of all status files for the main proc and workers.
    my @status_files;
    push(@status_files, glob(sprintf("'{%s}status.txt'", $self->status_dir)));
    push(@status_files, glob(sprintf("'{%s}*status.txt'", $self->worker_dir)));

    # Remove all of the status files.
    for my $file (@status_files) {
        unless (-f $file) {
            $self->logger->error("Can't remove abnormal file: $file");
            next;
        }
        unlink($file) or $self->logger->error(sprintf("Can't delete %s: %s", $file, $!));
    }
    $self->logger->debug("Deleted old simp-tsds status files");
}

=item _get_status_data()
    Opens, reads, closes, and decodes a JSON status file in order.
    When successful, a hash of status data is returned.
    If errors are encountered, undef is returned.
=cut
sub _get_status_data {
    my ($self, $pid, $collection) = @_;

    # A hash of status data is returned.
    # Returned undef when data could not be read.
    my $status_data;

    # Get the worker's status file using name and PID.
    my $file = $self->worker_dir . sprintf("%s-%s-status.txt", $pid, $collection->{'name'});

    # Check that the status file exists and is readable
    if (-e $file && -r $file) {

        # Open the status file.
        if(open(my $fh, "<", $file)) {
            
            # Read the JSON string
            my $status_json = readline($fh);

            # Close the file or log an error on failure
            unless (close($fh)) {
                $self->logger->error(sprintf("Could not close status file %s. Error: %s", $file, $!));
            }

            # Try to decode the JSON string into a status data hash.
            try {
                $status_data = decode_json($status_json);
            } catch {
                $self->logger->error(sprintf("Could not decode status file JSON from %s. Error: %s", $file, $_));
            };
        }
        # Failed to open the file.
        else {
            $self->logger->error(sprintf("Could not open status file %s. Error: %s", $file, $!));
        }
    }
    # File doesn't exist or can't be read using current permissions
    else {
        $self->logger->error("Could not open status file %s. Error: Doesn't exist or wrong permissions.", $file);
    }

    return $status_data;
}


# Checks status files of workers spawned by this master process Writes an aggregate
# (okay | not okay) status file at /var/lib/grnoc/simp-tsds-master, including how.
# many worker status files had indicated errors This master status file is monitored.
# by nrped.
sub _check_worker_health {
    my $self = shift;

    $self->logger->debug("Checking the health of Simp::TSDS workers");

    # Get the current time.
    my $now = time();

    # Track errors found in the status files.
    my $errors = 0;

    # Read and parse status files for all worker PIDs and names.
    while (my ($pid, $collection) = each(%{$self->workers})) {

        # Get the status data.
        my $status_data = $self->_get_status_data($pid, $collection);

        # No status data could be found or parsed.
        # Track the error and skip to the next worker.
        unless ($status_data) {
            $errors++;
            next;
        }

        # Check for errors in the status data.
        if ($status_data->{'error'} ne 0) {
            $self->logger->error(sprintf("Non-zero error flag from %s. Error: %s",
                $collection->{'name'},
                $status_data->{'error_text'})
            );
            $errors++; 
        }

        # Check that timestamp is fresh (timestamp within 2 * interval of current time).
        if ($status_data->{'timestamp'} < ($now - $collection->{'interval'} * 2)) {
            $self->logger->error(sprintf("Stale status file from %s - \n current time: %s \n interval: %s \n latest timestamp: %s",
                $collection->{'name'},
                $now,
                $collection->{'interval'},
                $status_data->{'timestamp'})
            );
            $errors++; 
        }
    }

    # Create a status string and write it to the main status file.
    my $status = "";

    if ($errors) {
        $status = sprintf(
            "simp-tsds found %s of %s workers reporting errors or with stale/missing status files in %s",
            $errors,
            $self->total_workers,
            $self->worker_dir
        );
        $self->logger->error($status);
    }

    my $res = write_service_status(
        service_name    => 'simp-tsds',
        error           => ($errors) ? 1 : 0,
        error_txt       => $status,
    );
    $self->logger->error("Couldn't write main status file");
}


=head2 _log_exit()
    Does exactly what the name implies
=cut
sub _log_exit {
    my $self = shift;
    my $msg  = shift;
    $self->logger->error($msg) and exit(1);
}


=head2 PUBLIC METHODS
=item start()
    Starts the main process loop that requests data from Simp::Comp.
=cut
sub start {
    my ($self) = @_;

    # Daemonized
    if ($self->daemonize) {

        $self->logger->debug('Starting simp-tsds in daemonized mode');

        my $daemon = Proc::Daemon->new(pid_file => $self->pid_file);
        my $pid    = $daemon->Init();

        # Exit from the initial simp-tsds.pl process.
        return if ($pid);

        $self->logger->debug('Created daemon process for simp-tsds');

        # Set the process name.
        $0 = "simp_tsds [master]";

        # Figure out what user/group (if any) to change to.
        my $user_name  = $self->run_user;
        my $group_name = $self->run_group;

        # Set the process' group.
        if (defined($group_name)) {
            my $gid = getgrnam($group_name);
            $self->_log_exit("Unable to get GID for group '$group_name'") unless (defined($gid));
            $! = 0;
            setgid($gid);
            $self->_log_exit("Unable to set GID to $gid ($group_name)") unless ($! == 0);
        }

        # Set the process' user.
        if (defined($user_name)) {
            my $uid = getpwnam($user_name);
            $self->_log_exit("Unable to get UID for user '$user_name'") unless (defined($uid));
            $! = 0;
            setuid($uid);
            $self->_log_exit("Unable to set UID to $uid ($user_name)") unless ($! == 0);
        }
    }
    # Foreground
    else {
        $self->logger->info("Starting simp-tsds in foreground mode");
    }

    # Setup signal handlers.
    # TERM, INT, and HUP handlers reap all child procs, unblocking the main loop.
    # On HUP, children are reaped and do_reload is set to let the loop continue.
    $SIG{'TERM'} = sub {
        $self->logger->info('Received SIG TERM.');
        $self->stop();
    };
    $SIG{'INT'} = sub {
        $self->logger->info('Received SIGINT.');
        $self->stop();
    };
    $SIG{'HUP'} = sub {
        $self->logger->info('Received SIG HUP.');
        $self->_set_do_reload(1);
        $self->stop();
    };

    # The main process loop for Simp::TSDS to start workers and request data.
    while (1) {
        
        # Read and set configurations required by the worker procs.
        $self->_process_config();
        $self->_process_collections();

        # Create the worker procs and their health watcher.
        # Note: The loop is blocked here until all worker procs are reaped.
        $self->_create_workers();

        # We only arrive here if the loop is running or simp-tsds is killed.
        $self->logger->info("Simp::TSDS has exited successfully");

        last if (!$self->do_reload);
        $self->_set_do_reload(0);
    }
    return 1;
}

=item stop()
    Reaps all of the Simp::TSDS::Worker child processes.
    Clears the worker cache once processes have stopped.
=cut
sub stop {
    my ($self) = @_;
    $self->logger->info('Stopping.');

    # Get all of the worker process PIDs from the worker cache.
    my @pids = keys(%{$self->workers});
    $self->logger->info('Stopping child worker processes ' . join(' ', @pids) . '.');

    # Reap all worker procs and wait for them to finish exiting.
    my $res = kill('TERM', @pids);

    # Clear the worker cache after all worker procs have exited.
    $self->_set_workers({});
}
1;
