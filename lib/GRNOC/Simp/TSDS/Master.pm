package GRNOC::Simp::TSDS::Master;

use strict;
use warnings;

use AnyEvent::Subprocess;
use Data::Dumper;
use File::Basename;
use Log::Log4perl;
use Moo;
use JSON::XS qw(decode_json);
use POSIX qw(setuid setgid);
use Proc::Daemon;
use Types::Standard qw(Str Bool Int);

use GRNOC::Monitoring::Service::Status;
use GRNOC::Config;
use GRNOC::RabbitMQ::Client;
use GRNOC::Simp::TSDS::Worker;

=head2 public attributes

=over 12

=item config

=item collections_dir

=item pidfile

=item daemonize

=item run_user

=item run_group

=item worker_status_dir

=item health_checker

=item status_filepath

=back

=cut

has config => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

has collections_dir => (
    is       => 'ro',
    isa      => Str,
    required => 1,
    default  => "/etc/simp/tsds/collections.d/"
);

has validation_dir => (
    is       => 'ro',
    isa      => Str,
    required => 1,
    default  => '/etc/simp/tsdds/validation.d/'
);

has pidfile => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

has daemonize => (
    is       => 'ro',
    isa      => Bool,
    required => 1
);

has run_user => (
    is       => 'ro',
    required => 0
);

has run_group => (
    is       => 'ro',
    required => 0
);

has worker_status_dir => (
    is       => 'rwp',
    required => 0
);

has health_checker => (
    is       => 'rwp'
);

has status_filepath => (
    is       => 'rwp',
);

=head2 private attributes

=over 12

=item logger

=item rabbitmq

=item tsds_instance

=item collections

=item worker_client

=item children

=item hup

=item stagger_interval

=back

=cut

has logger => (is => 'rwp');

has rabbitmq => (is => 'rwp');

has tsds_instance => (is => 'rwp');

has collections => (
    is      => 'rwp',
    default => sub { [] }
);

has worker_client => (is => 'rwp');

has children => (
    is      => 'rwp',
    default => sub { [] }
);

has workers => (
    is      => 'rwp',
    default => sub { [] }
);

has hup => (
    is      => 'rwp',
    default => 0
);

has stagger_interval => (
    is       => 'rwp',
    isa      => Int,
    required => 0,
    default  => 5,
);

my $running;

=head2 BUILD

=cut

sub BUILD
{
    my $self = shift;

    $self->_set_logger(Log::Log4perl->get_logger('GRNOC.Simp.TSDS.Master'));

    return $self;
}

=head2 start
    start the master process
=cut

sub start
{
    my ($self) = @_;

    $self->logger->info('Starting.');
    $self->logger->debug('Setting up signal handlers.');

    $0 = "simp_tsds [master]";

    # Daemonize if needed
    if ($self->daemonize)
    {
        $self->logger->debug('Daemonizing.');

        my $daemon = Proc::Daemon->new(
            pid_file     => $self->pidfile,
            child_STDOUT => '/tmp/oess-vlan-collector.out',
            child_STDERR => '/tmp/oess-vlan-collector.err'
        );
        my $pid = $daemon->Init();

        if ($pid)
        {
            sleep 1;
            die 'Spawning child process failed' if !$daemon->Status();
            exit(0);
        }
    }

    warn "Child is alive!!\n";

    # If requested, change to different user and group
    if (defined($self->run_group))
    {
        my $run_group = $self->run_group;
        my $gid       = getpwnam($self->run_group);
        die "Unable to get GID for group '$run_group'\n" if !defined($gid);
        $! = 0;
        setgid($gid);
        die "Unable to set GID to '$run_group' ($gid): $!\n" if $! != 0;
    }

    if (defined($self->run_user))
    {
        my $run_user = $self->run_user;
        my $uid      = getpwnam($run_user);
        die "Unable to get UID for user '$run_user'\n" if !defined($uid);
        $! = 0;
        setuid($uid);
        die "Unable to set UID to '$run_user' ($uid): $!\n" if $! != 0;
    }

    # Only run once unless HUP gets set, then reload and go again
    while (1)
    {
        $self->_set_hup(0);
        $self->_load_config();
        $self->_create_workers();

        last unless $self->hup;
    }

    $self->logger->info("Master terminating");
}

# Helper function that returns a GRNOC Config object from a file
sub _get_conf
{
    my $self = shift;
    return GRNOC::Config->new(config_file => shift, force_array => shift || 1);
}

sub _validate_config
{
    my $self   = shift;
    my $file   = shift;
    my $config = shift;
    my $xsd    = shift;

    # Validate the config
    my $validation_code = $config->validate($xsd);

    # Use the validation code to log the outcome and exit if any errors occur
    if ($validation_code == 1)
    {
        $self->logger->debug("Successfully validated " . $file);
        return 1;
    }
    else
    {
        if ($validation_code == 0)
        {
            $self->logger->error("ERROR: Failed to validate "
                  . $file . "!\n"
                  . $config->{error}->{backtrace});
        }
        else
        {
            $self->logger->error("ERROR: XML schema in $xsd is invalid!\n"
                  . $config->{error}->{backtrace});
        }

        exit(1);
    }

}

# Load config and set up Master object
sub _load_config
{
    my ($self) = @_;

    $self->logger->info("Reading configuration from " . $self->config);

    # Get the settings for the RabbitMQ server,
    # TSDS services instance, and stagger time
    my $config = $self->_get_conf($self->config);

    # Get the validation file for config.xml
    my $config_xsd = $self->validation_dir . 'config.xsd';

    # Validate the config file or exit
    $self->_validate_config($self->config, $config, $config_xsd);

    # Set parameters from the config
    $self->_set_rabbitmq($config->get('/config/rabbitmq')->[0]);
    $self->_set_tsds_instance($config->get('/config/tsds')->[0]);
    $self->_set_stagger_interval($config->get('/config/stagger/@seconds')->[0]);

    # Read all collections XML files from the collections.d directory
    opendir my $dir, $self->collections_dir
      or $self->logger->error("Could not open $self->collections_dir");
    my @collections_files = grep { $_ =~ /^[^.#][^#]*\.xml$/ } readdir $dir;
    closedir $dir;

    # Set collections to an array of collection XPath
    # config objects assigned to their filename
    my @collections;
    my $collection_xsd = $self->validation_dir . 'collection.xsd';
    for my $file (@collections_files)
    {
        my $collection_config =
          $self->_get_conf($self->collections_dir . $file);

        # Validate the collection file and exit if errors
        $self->_validate_config($file, $collection_config, $collection_xsd);

        for my $collection (@{$collection_config->get('/config/collection')})
        {
            $collection->{'host'} = [] if !defined($collection->{'host'});
            push(@collections, $collection);
        }
    }
    $self->_set_collections(\@collections);
    $self->_set_worker_client(undef);
}

# Creates and starts all collection workers
# When a TERM, INT, or HUP is received, the workers are told to quit once
# Returns once all workers have joined
sub _create_workers
{
    my $self = shift;

    my $global_offset = 0;

    # Set path for workers to write status files in
    $self->_set_worker_status_dir("/var/lib/grnoc/simp-tsds/workers");

    # Clear any old status files in the simp-tsds and worker directories
    my @old_files = glob("'/var/lib/grnoc/simp-tsds/status.txt'");
    push(@old_files, glob("'/var/lib/grnoc/simp-tsds/workers/*status.txt'"));
    for my $file (@old_files) {
        if ( -d $file ) {
            $self->logger->error("Unexpected directory $file caught by glob while trying to unlink old status files.");
            next;
        }
        unlink($file)
            or $self->logger->error("Can't unlink $file (expected old status file for deletion)");
    }

    # Schedule callback to check status files written by workers
    # NOTE: freshness of status file is checked per each worker's interval in _check_worker_health. 
    # The static interval below (15s) is meant to be the shortest interval reasonably expected of any collection for SIMP.
    $self->_set_health_checker(AnyEvent->timer(
        after       => 15,
        interval    => 15,
        cb          => sub { $self->_check_worker_health(); }
    ));

    # Create separate groups of workers for each collection
    for my $collection (@{$self->collections})
    {
        $global_offset += $self->_create_collection_workers($collection, $global_offset);
    }

    $running = AnyEvent->condvar;

    $SIG{'TERM'} = sub {
        $self->logger->info('Received SIGTERM.');
        for my $worker (@{$self->children})
        {
            $worker->kill();
        }
        exit;
    };

    $SIG{'INT'} = sub {
        $self->logger->info('Received SIGINT.');

        for my $worker (@{$self->children})
        {
            $worker->kill();
        }
        exit;
    };

    $SIG{'HUP'} = sub {
        $self->logger->info('Received SIGHUP.');
        $self->_set_hup(1);

        while (my $worker = pop @{$self->children})
        {
            $worker->kill();
            my $pid = $worker->child_pid();
            $self->logger->info("Waiting for $pid to exit...");
            waitpid($pid, 0);
            $self->logger->info("Child $pid has exited.");
        }

        $running->send;
    };

    $running->recv;
}

# Creates workers for a single collection
sub _create_collection_workers
{
    my ($self, $collection, $offset) = @_;
    my %worker_hosts;

    # Divide up hosts in config among number of workers defined in config
    my $i = 0;
    for my $host (@{$collection->{'host'}})
    {
        push(@{$worker_hosts{$i}}, $host);

        $i++;

        if ($i >= $collection->{'workers'})
        {
            $i = 0;
        }
    }

    # Spawn workers
    for my $worker_id (keys %worker_hosts)
    {
        my $worker_name = "$collection->{'composite'} [$worker_id]";

        $self->_create_worker(
            name       => $worker_name,
            collection => $collection,
            hosts      => $worker_hosts{$worker_id},
	        offset     => $offset++
        );
    }

    return $offset;
}

# Creates an individual worker
sub _create_worker
{
    my $self   = shift;
    my %params = @_;

    my $collection     = $params{'collection'};
    my $stagger_offset = $params{'offset'};

    my $interval = $collection->{'interval'};
    my $stagger  = $self->stagger_interval();

    # Make sure stagger is between 0 and interval, doesn't make sense to
    # stagger 65s on a 60s collection, it's the same as 5s except much
    # slower to start up
    my $actual_offset = ($stagger_offset * $stagger) % $interval;

    my $required_vals =
        defined $collection->{required_values}
        ? $collection->{'required_values'}
        : '';

    my $excludes =
        defined $collection->{'exclude'} ? $collection->{exclude} : [];
    my @excludes = grep {
                defined($_->{'var'})
            && defined($_->{'pattern'})
            && (length($_->{'var'}) > 0)
    } @$excludes;
    @excludes = map { "$_->{'var'}=$_->{'pattern'}" } @excludes;

    $self->logger->info("Creating Collector for " . $params{'name'});
    my $worker = GRNOC::Simp::TSDS::Worker->new(
        worker_name      => $params{name},
        hosts            => $params{hosts},
        logger           => $self->logger,
        rabbitmq         => $self->rabbitmq,
        tsds_config      => $self->tsds_instance,
        composite        => $collection->{'composite'},
        measurement_type => $collection->{'measurement_type'},
        interval         => $collection->{'interval'},
        stagger_offset   => $actual_offset,
        filter_name      => $collection->{'filter_name'},
        filter_value     => $collection->{'filter_value'},
        required_values  => [split(',', $required_vals)],
        exclude_patterns => \@excludes,
        status_filepath  => $self->worker_status_dir,
    );
    push(@{$self->workers}, $worker);

    # TODO: The code below, for creating the AnyEvent sub, could be split out into another class subroutine i.e. _start_worker
    # This could then be called with a worker ref as an argument, allowing for a base to reimplement worker resusitation.
    # However, at the present moment, we see no reason for a worker to restart on an unexpected death.

    # Create a subroutine to fork the start of each new worker.
    my $init_proc = AnyEvent::Subprocess->new(
        on_completion => sub {
            $self->logger->error("Child " . $params{'name'} . " has died");
        },
        code => sub {
            $worker->start();
        }
    );
    # Start worker, 
    my $proc = $init_proc->run();
    push(@{$self->children}, $proc);
}

# Checks status files of workers spawned by this master process. Writes an aggregate 
# (okay | not okay) status file at /var/lib/grnoc/simp-tsds-master, including how
# many worker status files had indicated errors. This master status file is monitored
# by nrpe.d
sub _check_worker_health() {
    my $self = shift;
    my $dir = $self->worker_status_dir."/*status.txt";
    my %files = map { basename($_) => $_} glob($dir); 

    my $errors_found = 0;
    # Loop through all worker names we expect to find status files for
    foreach my $worker (@{$self->workers}) {
        my $filename = $worker->worker_name . "status.txt"; 
        if ( exists $files{$filename} ) {
            my $res = open(my $fh, "<$files{$filename}");
            if ( $res ) {
                my $worker_status = readline($fh);
                $worker_status = decode_json($worker_status);
                # Check that status file has a clear error flag i.e. 0
                if ( $worker_status->{'error'} ne 0 ) {
                    $self->logger->error(sprintf("Non-zero error flag from %s. Error: %s",
                        $worker->worker_name,
                        $worker_status->{'error_text'})
                    );
                    $errors_found += 1; 
                } 
                # Check that timestamp is fresh (timestamp within 2 * interval of current time)
                if ( $worker_status->{'timestamp'} < (time() - $worker->interval * 2) ) {
                    $self->logger->error(sprintf("\n Stale status file from %s - \n current time: %s \n interval: %s \n latest timestamp: %s",
                        $worker->worker_name,
                        time(),
                        $worker->interval,
                        $worker_status->{'timestamp'})
                    );
                    $errors_found += 1; 
                }
            # Any missing file or unreadable file needs to be investigated as an error
            } else {
                $self->logger->error("Could not open status file for worker $worker.");
                $errors_found += 1;
            }
        } else {
            $self->logger->error("Could not find a status file for worker $worker");
            $errors_found += 1;
        }
    }

    my $res = write_service_status(
        service_name    => 'simp-tsds',
        error           => ($errors_found > 0) + 0,
        error_txt       => ($errors_found > 0 ? "simp-tsds found $errors_found errors or stale/missing status files in ".$self->worker_status_dir : "")
    );
    if (!$res) {
        # Should this cause an exit(), since monitoring will be broken?
        $self->logger->error("Problem writing master status file!");
    }
}

1;
