package GRNOC::Simp::Data;

use strict;
use warnings;

use Moo;
use Parallel::ForkManager;
use POSIX qw( setuid setgid );
use Proc::Daemon;
use Types::Standard qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Simp::Data::Worker;

our $VERSION = '1.8.0';

=head2 public attributes
=over 12

=item config_file
=item logging_file
=item daemonize
=item run_user
=item run_group

=back
=cut
# Required Attributes
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
has validation_file => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

# Optional Attributes
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

=head2 private attributes
=over 12

=item config
=item logger
=item children

=back
=cut

has config => (is => 'rwp');
has logger => (is => 'rwp');
has children => (
    is      => 'rwp',
    default => sub {[]}
);


=head2 BUILD
    Creates the main Poller Moo object and process
=cut
sub BUILD {
    my ($self) = @_;

    # Create and store logger object
    my $grnoc_log = GRNOC::Log->new(config => $self->logging_file, watch => 120);
    my $logger    = GRNOC::Log->get_logger();
    $self->_set_logger($logger);

    # Create the config object
    my $config = GRNOC::Config->new(
        config_file => $self->config_file,
        force_array => 0
    );

    # Validate the config, exiting if there are errors
    my $validation_code = $config->validate($self->validation_file);

    if ($validation_code == 1) {
        $self->logger->debug("Successfully validated config file");
    }
    else {
        if ($validation_code == 0) {
            $self->logger->error("ERROR: Failed to validate $self->config_file!\n" . $config->{error}->{backtrace});
        }
        else {
            $self->logger->error("ERROR: XML schema in $self->validation_file is invalid!\n" . $config->{error}->{backtrace});
        }
        exit(1);
    }

    # Store the config object once it's been validated
    $self->_set_config($config);

    return $self;
}


=head2 start
    Starts the entire Poller main process and workers
=cut
sub start {

    my ($self) = @_;

    $self->logger->info('Starting.');
    $self->logger->debug('Setting up signal handlers.');

    # Set the signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info('Received SIG TERM.');
        $self->stop();
    };
    $SIG{'HUP'} = sub {
        $self->logger->info('Received SIG HUP.');
    };

    # Daemonized
    if ($self->daemonize) {
        $self->logger->debug('Daemonizing.');

        # Get the PID file
        my $pid_file = $self->config->get('/config/@pid-file') || "/var/run/simp_data.pid";
        $self->logger->debug("PID FILE: " . $pid_file);

        # Create a daemon using the PID file
        my $daemon = Proc::Daemon->new(pid_file => $pid_file);

        # Initialize the daemon process
        my $pid = $daemon->Init();

        # PID will be falsy on successful startup
        if (!$pid) {
            $self->logger->debug('Created daemon process.');

            # Change the process name
            $0 = "simp_data [master]";

            # Get the user and group to run the process as (if specified)
            my $user_name  = $self->run_user;
            my $group_name = $self->run_group;

            # Change the process' group
            if (defined($group_name)) {
                my $gid = getgrnam($group_name);
                $self->_log_err_then_exit("Unable to get GID for group '$group_name'") unless (defined($gid));
                $! = 0;
                setgid($gid);
                $self->_log_err_then_exit("Unable to set GID to $gid ($group_name)") unless ($! == 0);
            }

            # Change the process' user
            if (defined($user_name)) {
                my $uid = getpwnam($user_name);
                $self->_log_err_then_exit("Unable to get UID for user '$user_name'") unless (defined($uid));
                $! = 0;
                setuid($uid);
                $self->_log_err_then_exit("Unable to set UID to $uid ($user_name)") unless ($! == 0);
            }

            # Create all of the Poller.Worker processes
            $self->_create_workers();
        }
    }
    # Foreground
    else {
        $self->logger->debug('Running in foreground.');

        # Create and start a single worker in foreground mode, this helps with profiling
        my $worker = GRNOC::Simp::Data::Worker->new(
            config    => $self->config,
            logger    => $self->logger,
            worker_id => 13
        );

        # Start the worker
        # This will only return if we tell it to stop via signal handlers
        $worker->start();
    }
    return 1;
}


=head2 _log_err_then_exit()
    Logs an error message and exits the program.
=cut
sub _log_err_then_exit {
    my $self = shift;
    my $msg  = shift;

    $self->logger->error($msg);
    warn "$msg\n";
    exit 1;
}


=head2 stop
    Stops the main process and all its workers.
=cut
sub stop {
    my ($self) = @_;

    $self->logger->info('Stopping.');

    # Get the array of PIDs for the workers
    my @pids = @{$self->children};
    $self->logger->debug('Stopping child worker processes ' . join(' ', @pids) . '.');

    # Kill the child processes using SIG TERM
    return kill('TERM', @pids);
}


=head2 _create_workers()
    Creates all of the worker processes requested by config.
=cut
sub _create_workers {
    my ($self) = @_;

    # Get the number of daemonized workers from config
    my $workers = $self->config->get('/config/@workers');
    $self->logger->info("Creating $workers child worker processes.");

    # Create a forked process manager for the number of workers
    my $forker = Parallel::ForkManager->new($workers);

    # Track the worker PIDs as they're started
    $forker->run_on_start(
        sub {
            my ($pid) = @_;
            $self->logger->debug("Worker process $pid created.");
            push(@{$self->children}, $pid);
        }
    );

    # Create the worker processes using the forker
    for (my $worker_id = 0; $worker_id < $workers; $worker_id++) {

        # Start the child process for the worker
        $forker->start() and next;

        # Create the worker object for the process
        my $worker = GRNOC::Simp::Data::Worker->new(
            config    => $self->config,
            logger    => $self->logger,
            worker_id => $worker_id
        );

        # Start the worker, only returns when exited via signal handler
        $worker->start();

        # Exit the child process
        $forker->finish();
    }

    # Wait for all forked child processes to exit
    $self->logger->debug('Waiting for all child worker processes to exit.');
    $forker->wait_all_children();

    # Clear the array of child processes
    $self->_set_children([]);
    $self->logger->debug('All child workers have exited.');
}

1;
