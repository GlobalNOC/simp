package GRNOC::Simp::TSDS;

our $VERSION = '1.11.2';

# Documentation commands in Perl Pod format.
# see: https://perldoc.perlorg/perlpod.

=head1 GRNOC::SIMP::TSDS
    The main module providing a Simp collector to TSDS DB interface.
    Simp::TSDS requests data collections from GRNOC::Simp::Comp via AMQP.
    GRNOC::Simp::TSDS::Worker procs initiate requests unless run with --nofork.
    Specific collections are set via collection.xml configurations.
=cut

use strict;
use warnings;

use Moo;
use Try::Tiny;
use Proc::Daemon;
use Parallel::ForkManager;
use POSIX qw(setuid setgid);
use JSON::XS qw(encode_json);

use GRNOC::Log;
use GRNOC::Config;
use GRNOC::Simp::TSDS::Worker;
use GRNOC::Monitoring::Service::Status;

# FOR DEVELOPMENT USE ONLY
# use Data::Dumper;
# $Data::Dumper::Indent = 1;

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
=item forker
=item workers
=item push_size
=item collections
=item status_cache
=item unsent_cache
=back
=cut
has config         => (is => 'rwp');
has logger         => (is => 'rwp');
has rmq_config     => (is => 'rwp');
has tsds_config    => (is => 'rwp');
has forker         => (is => 'rwp');
has max_workers    => (is => 'rwp', default => 4);
has dump_threshold => (is => 'rwp', default => 10000);
has collections    => (is => 'rwp', default => sub { [] });
has status_cache   => (is => 'rwp', default => sub { {} });
has unsent_cache   => (is => 'rwp', default => sub { [] });

=head2 PUBLIC METHODS
=item BUILD
    Initialization method for the Moo Simp::TSDS class.
    This runs when GRNOC::Simp::TSDS->new() is called.
=cut
sub BUILD {
    my $self = shift;
    $self->_setup_logging();
    return $self;
}

=item run()
    Runs the main process loop that requests data from Simp::Comp.
=cut
sub run {
    my ($self) = @_;

    # Daemonized
    if ($self->daemonize) {
        $self->logger->debug('Starting simp-tsds in daemonized mode');

        my $daemon = Proc::Daemon->new(pid_file => $self->pid_file);
        my $pid    = $daemon->Init({});

        # Exit from the simp-tsds.pl launcher process.
        return if ($pid);

        # Set the daemon process name.
        $0 = "simp_tsds [daemon]";

        # Change the user and group if needed.
        my $user  = $self->run_user;
        my $group = $self->run_group;

        # Set the process' group.
        if (defined($group)) {
            my $gid = getgrnam($group);
            $self->_log_exit("Unable to get GID for group '$group'") unless (defined($gid));
            $! = 0;
            setgid($gid);
            $self->_log_exit("Unable to set GID to $gid ($group)") unless ($! == 0);
        }

        # Set the process' user.
        if (defined($user)) {
            my $uid = getpwnam($user);
            $self->_log_exit("Unable to get UID for user '$user'") unless (defined($uid));
            $! = 0;
            setuid($uid);
            $self->_log_exit("Unable to set UID to $uid ($user)") unless ($! == 0);
        }
    }
    # Foreground
    else {
        # Set the process name.
        $0 = "simp_tsds [foreground]";
        $self->logger->info("Starting simp-tsds in foreground mode");
    }

    # Set up signal handlers.
    # TERM and INT signals cause Simp::TSDS to exit.
    $SIG{'TERM'} = sub {
        $self->logger->info('Received SIG TERM.');
        exit(0);
    };
    $SIG{'INT'} = sub {
        $self->logger->info('Received SIG INT.');
        exit(0);
    };
    # HUP signal causes Simp::TSDS to reload all configs and caches.
    $SIG{'HUP'} = sub {
        $self->logger->info('Received SIG HUP.');
        $self->_setup();
    };

    # Set up all the required configurations, caches, and objects.
    # NOTE: _setup() MUST occur after any daemonization for P::FM.
    $self->_setup();

    # The process loop for Simp::TSDS to start workers and request data.
    while (1) {
        $self->_collect();
        sleep(1);
    }

    # The main loop should only exit on TERM or INT.
    exit(70);
}

=head2 PRIVATE METHODS
=item _setup()
    Wrapper method that loads all configurations sets up caches.
    This should be run any time a configuration change is made.

    Note:
    MUST run after forking to daemon for Parallel::ForkManager.
    P::FM will use the current PID as the main/parent PID.
    This breaks child->parent data when the current PID is not the daemon's.
=cut
sub _setup {
    my ($self) = @_;
    $self->_setup_logging();
    $self->_setup_config();
    $self->_setup_collections();
    $self->_setup_status_cache();
    $self->_setup_forker();
    $self->logger->info('Finished loading');
}

=item _setup_logging()
    Creates the GRNOC::Log logger and caches it in $self.
=cut
sub _setup_logging {
    my ($self) = @_;
    my $log    = GRNOC::Log->new(config => $self->logging_file, watch => 120);
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger($logger);
    $self->logger->debug("Set the logger");
}

=item _validate_config()
    Validates a GRNOC::Config object using an XSD file.
    Requires the filename for logging purposes.
    Returns true for valid configs else undef.
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

=item _setup_config()
    Loads and processes the main Simp::TSDS configuration.
    Sets the RabbitMQ, TSDS, and config private attributes.
=cut
sub _setup_config {
    my ($self) = @_;
    $self->logger->info(sprintf("Reading config %s", $self->config_file));

    # Create the main config object.
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

    # Set the config objects for RabbitMQ and the TSDS API
    $self->_set_rmq_config($config->get('/config/rabbitmq')->[0]);
    $self->_set_tsds_config($config->get('/config/tsds')->[0]);

    # Set the global performance tuning settings 
    # Defaults set so reloaded config without these parameters uses them.
    my $settings = $config->get('/config/settings');
    $settings = $settings->[0] if ($settings);
    my $max_workers    = $settings->{'max_workers'} || 4;
    my $dump_threshold = $settings->{'dump_threshold'} || 10000;
    $self->_set_max_workers($max_workers);
    $self->_set_dump_threshold($dump_threshold);

    $self->logger->debug("Set the config");
}

=item _get_collection_configs()
    Loads the collections.d configuration files.
=cut
sub _get_collection_configs {
    my ($self) = @_;
    $self->logger->debug(sprintf("Reading collection configurations in %s", $self->collections_dir));

    # Array of collection config objects to return
    my @collection_configs;

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

    # Get a config object for each collection config
    for my $file (@config_files) {
        $self->logger->debug(sprintf("Processing %s", $file));

        # Create a new config object for the file.
        my $config = GRNOC::Config->new(
            config_file => sprintf("%s%s", $self->collections_dir, $file),
            force_array => 1
        );

        # Validate the collection config and keep it if valid.
        push(@collection_configs, $config) if ($self->_validate_config($file, $config, $xsd));
    }

    $self->logger->debug("Finished reading collection configurations");
    return \@collection_configs;
}

=item _setup_collections()
    Loads and processes the collections.d configurations.
    These are used in the creation of Simp::TSDS::Worker procs.
=cut
sub _setup_collections {
    my ($self) = @_;
    $self->logger->debug(sprintf("Setting up collections"));

    # Create a hash of collections grouped by interval
    # This hash is returned as a flat array of its values
    my %collections;

    # Get the current time to set the next cycle of each collection.
    # Collections are instantiated to $now so they run on start up.
    my $now = time();

    for my $config (@{$self->_get_collection_configs()}) {

        # Process each collection in the config and push the result to collections.
        for my $collection (@{$config->get('/config/collection')}) {

            # Get the interval for the collection's requests
            my $interval = $collection->{'interval'};

            # Initialize the next cycle as the current round-minute interval.
            # This ensures that cycles stay rounded and arent missed during restart.
            # Simp::TSDS::Worker will always request data back one interval from this.
            my $next_cycle = $now - ($now % $interval);

            # Initialize the collection's cached configurations.
            # A collection has an interval, next collection cycle, and requests array.
            unless (exists($collections{$interval})) {
                $collections{$interval} = {
                    interval   => $interval,
                    next_cycle => $next_cycle,
                    requests   => []
                };
            }

            # Get the collection's optional request args.
            my $required   = $collection->{'required_values'};
            my $exclusions = $collection->{'exclude'};
            my $filter_k   = $collection->{'filter_name'};
            my $filter_v   = $collection->{'filter_value'};
            my $filter;

            # Set any required values to an array using the CSV string
            if (defined($required)) {
                $required = [split(',', $required)];
            }

            # Set any array of any exclusion rule strings for the Simp.Comp queue
            if (defined($exclusions)) {
                $exclusions = [grep { defined($_->{'var'}) && defined($_->{'pattern'}) && (length($_->{'var'}) > 0) } @$exclusions];
                $exclusions = [map { "$_->{'var'}=$_->{'pattern'}" } @$exclusions];
            }

            # Set any filter KVP for the Simp.Comp queue
            if (defined($filter_k) && defined($filter_v)) {
                $filter = {name => $filter_k, value => $filter_v};
            }

            # A collection's hosts each get a request hash for the interval.
            for my $host (@{$collection->{'host'}}) {

                # Initialize a request hash with required args.
                # This will be used by a Simp::TSDS::Worker to gather and send data.
                my %request = (
                    node        => $host,
                    interval    => $interval,
                    composite   => $collection->{'composite'},
                    measurement => $collection->{'measurement_type'},
                );

                # Generate a composite string ID for the request BEFORE adding optional args.
                # The ID is a pipe-dilineated string of values for the sorted request hash keys.
                # This is used for lookups in status_cache.
                $request{'id'} = join('|', map { $request{$_} } sort(keys(%request)));

                # Add optional args if defined
                $request{'required'}   = $required if ($required);
                $request{'exclusions'} = $exclusions if ($exclusions);
                $request{'filter'}     = $filter if ($filter);

                # Add the request to the interval
                push(@{$collections{$interval}{'requests'}}, \%request);
            }
        }
    }

    # Collection parameters were built as a hash of collection hashes keyed on interval.
    # Unpack the interval hashes into an array.
    my $collections_array = [values(\%collections)];

    # Set the array of collection configs.
    $self->_set_collections($collections_array);
    $self->logger->debug("Finished setting up collections");
}

=item _setup_status_cache()
    Initializes the status cache for all requests after _setup_collections().
    This will also clear any cached status data during a reload (HUP).
=cut
sub _setup_status_cache {
    my ($self) = @_;
    $self->logger->debug("Setting up status cache");

    # Create a hash to use as the status cache.
    # Workers is the total number of workers for a cycle.
    # Errors, Rabbitmq, and TSDS values are counts of workers with flagged errors.
    # Unsent is counter of total unsent messages (DO NOT CONFUSE WITH $self->unsent_cache)
    # Duration is sum of worker durations, used to calculate cycle average.
    # Dump is the number of messages that have failed to dump to disk.
    # Requests will be a hash of request ID's to their individual status data.
    my $status_cache = {
        'errors'   => 0,
        'rabbitmq' => 0,
        'tsds'     => 0,
        'duration' => 0,
        'dump'     => 0,
        'requests' => {},
    };

    # Set the resulting hash as the status cache.
    # NOTE: This will clear any previously cached status data.
    $self->_set_status_cache($status_cache);
    $self->logger->debug("Set status cache");
}

=item _setup_forker()
=cut
sub _setup_forker {
    my ($self) = @_;

    # Create a fork manager with a max forks limit.
    my $forker = Parallel::ForkManager->new($self->max_workers);

    # Callback for worker procs to run when reaped.
    $forker->run_on_finish(sub {
        my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $status) = @_;

        # Ensure the worker sent a valid status hash.
        if (defined($status) && ref($status) eq 'HASH') {

            # This caches the worker's status data in $self->status_cache.
            # It also caches unsent messages in $self->unsent_cache.
            $self->_cache_worker_status($status);
        }
        else {
            $self->logger->error("Received invalid worker status: " . ref($status));
        }
    });

    # Cache the forker in $self to manage worker process forks.
    $self->_set_forker($forker);
}

=item _cache_worker_status()
=cut
sub _cache_worker_status {
    my ($self, $status) = @_;
    $self->logger->info(sprintf('%s: Finished %s requests in %s seconds', $status->{'name'}, scalar(keys(%{$status->{'requests'}})), $status->{'duration'}));

    # Add status counters to the totals.
    for my $counter (('errors', 'rabbitmq', 'tsds', 'duration')) {
        $self->status_cache->{$counter} += $status->{$counter};
    }

    # Cache each requests' status hash using its ID.
    while (my ($id, $request_status) = each(%{$status->{'requests'}})) {
        $self->status_cache->{'requests'}{$id} = $request_status;
    }

    # Cache messages the unsent messages for resending.
    push(@{$self->unsent_cache}, @{$status->{'unsent'}});
}

=head2 _balance_requests
    Creates an array of host config assignments per worker given a collection.
    The array index is the worker ID and its element is the array of configs.
    This can't be run until all collection configs have been processed.
=cut 
sub _balance_requests {
    my ($self, $requests) = @_;
    $self->logger->debug(sprintf("Balancing %s request load across %s workers", scalar(@$requests), $self->max_workers));

    # Create an array of arrays to store requests for each worker.
    # The array index serves as the worker ID number.
    my @worker_requests = map {[]} (0 .. ($self->max_workers - 1));

    # Track the worker ID getting host assignments
    my $id = 0;

    # Assign hosts to workers evenly (round-robin).
    for my $request (@$requests) {

        # Add the host to the worker's host array
        push(@{$worker_requests[$id]}, $request);

        # Update ID to the next worker.
        # Reset to 0 when new ID/index is past the last worker.
        $id++;
        $id = 0 if ($id > $#worker_requests);
    }
    return \@worker_requests;
}

=item _collect
=cut
sub _collect {
    my ($self) = @_;

    # Build an array of all requests ready this cycle.
    my $cycle_requests = [];
    for my $collection (@{$self->collections}) {

        # Check whether the collection is ready for a cycle.
        if (time() >= $collection->{'next_cycle'}) {
            $self->logger->info(sprintf("Running %ss collections", $collection->{'interval'}));

            # Add the collection's requests to this cycle.
            push(@$cycle_requests, @{$collection->{'requests'}});

            # Update the collection's next cycle to its next interval.
            $collection->{'next_cycle'} += $collection->{'interval'};
        }
    }
    
    # Exit early if there are no requests ready to process this cycle.
    return unless (scalar(@$cycle_requests));

    # Balance the request and any unsent message loads across the workers.
    my $worker_requests = $self->_balance_requests($cycle_requests);
    my $unsent_messages = $self->_balance_requests($self->unsent_cache);

    # Clear the cache of unsent messages.
    # If a retry fails, they will be cached again by workers.
    $self->_set_unsent_cache([]);

    # Spawn worker procs to handle the requests
    for my $id (0 .. $#$worker_requests) {

        # Get the new requests and unsent messages allocated to the worker ID
        my $requests = $worker_requests->[$id];
        my $unsent   = $unsent_messages->[$id];

        # Create a fork for the worker process
        my $pid = $self->forker->start;

        # PID is 0 when checked from the worker process.
        # PID is $$ when checked from the main process.
        # Parent process continues the loop without running the rest of the code
        next if ($pid);

        # Create the worker within the forked process
        my $worker = GRNOC::Simp::TSDS::Worker->new(
            id          => $id,
            requests    => $requests,
            unsent      => $unsent,
            logger      => $self->logger,            
            rmq_config  => $self->rmq_config,
            tsds_config => $self->tsds_config,
        );

        # Process the requests and return a status data hashref.
        my $worker_status = $worker->run();

        # Exit when the fork has finished, returning data from the worker.
        $self->forker->finish(1, $worker_status);
    }

    # Block here until all spawned worker procs have exited.
    $self->forker->wait_all_children();
    $self->logger->info('All workers have finished');

    my $average_duration = ($self->status_cache->{'duration'} / scalar(@$worker_requests));
    $self->logger->info(sprintf('Average worker duration: %.3fs', $average_duration));
    

    # Write a dump of unsent messages when past the threshold.
    $self->_write_unsent() if (scalar(@{$self->unsent_cache}) > $self->dump_threshold);

    # Write the status file.
    $self->_write_status(scalar(@{$worker_requests}));

    # Refresh the status_cache by rebuilding it.
    $self->_setup_status_cache();
}

sub _write_unsent {
    my ($self) = @_;
    $self->logger->info(sprintf('Unsent messages cached: %s (max: %s)', scalar(@{$self->unsent_cache}), $self->dump_threshold));

    # Do not attempt to write an empty array.
    return unless (scalar(@{$self->unsent_cache}));

    # Pull the data messages out of the unsent data objects.
    my @dump = map { $_->{'message'} } @{$self->unsent_cache};

    # Encode the array of data messages as JSON.
    my $json_dump;
    try {
        $json_dump = encode_json(\@dump);
    } catch {
        $self->logger->error(sprintf("Could not encode data as JSON during dump: %s", $_));
    };

    # Write the array of data messages to the dump dir.
    my $file = $self->status_dir . sprintf('dumps/dump_%s.json', time());

    # Write the file dump to disk and catch any I/O errors.
    my $write_error;
    open(FH, '>', $file) or $write_error = $!;
    print(FH $json_dump);
    close(FH) or $write_error = $!;

    # Clear the cache unless there were errors dumping it.
    unless ($write_error) {
        $self->logger->info(sprintf("Wrote %s message dump in %s", scalar(@dump), $file));
        $self->_set_unsent_cache([]);
    }
    else {
        $self->logger->error(sprintf("Could not write data dump: %s", $!));
        $self->status_cache->{'dump'} = scalar(@dump);
    }
}

=head3 _write_status
=cut
sub _write_status {
    my ($self, $workers) = @_;

    $self->logger->debug(sprintf('Writing status data to %sstatus.txt', $self->status_dir));

    # Get the current time and reference to the status cache.
    my $now   = time();
    my $cache = $self->status_cache;

    # Combine the status data for all requests
    my %request_summary = (
        'errors'   => 0,
        'rabbitmq' => 0,
        'tsds'     => 0,
        'encoding' => 0,
    );
    while (my ($id, $request_status) = each(%{$cache->{'requests'}})) {
        while (my ($flag, $value) = each(%$request_status)) {
            $request_summary{$flag} += $value;
        }
    }

    # Determine if there were any errors at the worker or request levels or with data dumps.
    my $errors_found = ($cache->{'errors'} + $request_summary{'errors'}) ? 1 : 0;

    # Initialize a status message (blank for success state).
    my $status_message = "";

    # Errors were found for workers or requests.
    if ($errors_found) {

        # Begin building our status message for the errors.
        $status_message = "ERROR: %s and %s";

        # Build a string for worker-level errors
        my $worker_message = sprintf(
            '%s of %s workers had errors [%s RabbitMQ | %s TSDS]',
            $cache->{'errors'},
            $workers,
            $cache->{'rabbitmq'},
            $cache->{'tsds'}
        );

        # Build a string for request-level errors
        my $request_message = sprintf(
            '%s requests had errors [%s RabbitMQ | %s TSDS | %s JSON]',
            $request_summary{'errors'},
            $request_summary{'rabbitmq'},
            $request_summary{'tsds'},
            $request_summary{'encoding'}
        );

        # Combine the messages
        $status_message = sprintf($status_message, $worker_message, $request_message);
    }
    # Special handling for errors related to dumping unsent messages.
    elsif ($cache->{'dump'}) {
        $status_message = sprintf("Could not dump %s unsent messages to %s", $cache->{'dump'}, $self->status_dir . 'dumps/');
        $errors_found   = 1;
    }

    my $write_result = write_service_status(
        service_name => 'simp-tsds',
        path         => $self->status_dir,
        error        => $errors_found,
        error_txt    => $status_message,
    );
    $self->logger->error("Could not write status file: $write_result") unless ($write_result);
}

=head2 _log_exit()
    Does exactly what the name implies
=cut
sub _log_exit {
    my $self = shift;
    my $msg  = shift;
    $self->logger->error($msg) and exit(1);
}
1;
