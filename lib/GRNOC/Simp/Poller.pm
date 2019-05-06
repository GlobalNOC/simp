package GRNOC::Simp::Poller;

use strict;
use warnings;
use File::Path 'rmtree';
use Data::Dumper;
use Moo;
use Types::Standard qw( Str Bool );

use Parallel::ForkManager;
use Proc::Daemon;
use POSIX qw( setuid setgid );

use GRNOC::Config;
use GRNOC::Log;

our $VERSION = '1.0.7';

use GRNOC::Simp::Poller::Worker;

### Required Attributes ###

=head2 public attributes

=over 12

=item config_file

=item logging_file

=item hosts_dir

=item status_dir

=item daemonize

=item run_user

=item run_group

=back

=cut

has config_file => ( 
    is => 'ro',
    isa => Str,
    required => 1
);

has logging_file => (
    is => 'ro',
    isa => Str,
    required => 1
);

has hosts_dir => (
    is => 'ro',
    isa => Str,
    required => 1
);

has status_dir => (
    is => 'rwp',
    isa => Str,
    required => 1
);

### Optional Attributes ###

has daemonize => (
    is => 'ro',
    isa => Bool,
    default => 1
);

has run_user => (
    is => 'ro',
    required => 0
);

has run_group => (
    is => 'ro',
    required => 0
);

### Private Attributes ###

=head2 private_attributes

=over 12

=item config

=item hosts

=item logger

=item status_path

=item children

=item do_reload

=back

=cut

has config => (
    is => 'rwp'
);

has logger => (
    is => 'rwp'
);

has hosts  => (
    is => 'rwp'
);

has children => (
    is => 'rwp',
    default => sub { [] }
);

has do_reload => (
    is => 'rwp', 
    default => 0
);

=head2 BUILD

=cut

sub BUILD {

    my ( $self ) = @_;

    # Create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file, watch => 120 );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # Create and store config objects
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 1 );
    $self->_set_config( $config );

    # Set status_dir to path in the configs, if defined and is a dir
    my $status_path = $self->config->get('/config/poller_status');

    my $status_dir;
    if ( defined $status_path ) {
        $status_dir = $status_path->[0]->{'dir'};
    }

    if ( defined $status_dir && -d $status_dir) {
        if ( substr($status_dir, -1) ne '/') {
            $status_dir .= '/';
            $self->logger->error("The path for status_dir didn't include a trailing slash, added it: $status_dir");
        }
        $self->_set_status_dir( $status_dir );
        $self->logger->debug("Found poller_status dir in config, using: " . $self->status_dir);

    # Use default if not defined in configs, or invalid
    } else {
        $self->logger->debug("No valid poller_status dir defined in config, using: " . $self->status_dir);
    }

    return $self;
}


=head2 _process_hosts_config

=cut

# Process the config files containing hosts to monitor
# the configs are located in /etc/simp/hosts.d/ by default
sub _process_hosts_config {

    my $self = shift;
    $self->logger->debug("BEGIN processing hosts_dir from config");

    # Open hosts.d/ and get array of files within, recursively
    opendir my $dir, $self->hosts_dir;
    my @files = readdir $dir;
    $self->logger->debug("Files in hosts_dir: " . Dumper(\@files));
    closedir $dir;

    my @hosts;

    # Loop through xml host config files and extract hosts
    foreach my $file (@files) {

        next if $file !~ /\.xml$/; # so we don't ingest editor tempfiles, etc.

        $self->logger->debug("Creating hosts_dir config path: " . $self->hosts_dir . $file);

        my $conf = GRNOC::Config->new(
            config_file => $self->hosts_dir . $file,
            force_array => 1
        );

        my $rawhosts = $conf->get("/config/host");

        foreach my $raw (@$rawhosts) {
            push(@hosts, $raw);
            $self->logger->debug("Host \"$raw->{node_name}\" added to \@hosts");

            # Check if status dir for each host exists, or create it.
            my $mon_dir = $self->status_dir . $raw->{node_name} . '/';
            unless ( -e $mon_dir || system("mkdir -m 0755 -p $mon_dir") == 0 ) {
                $self->logger->error("Could not find or create dir for monitoring data: $mon_dir");
            } else {
                $self->logger->debug("Found or created status dir for $raw->{node_name} successfully");
            }
        }
    }
   
    # Once status dirs for configured hosts have been made...
    # Remove any dirs that are not included in configurations
    for my $node_path ( glob($self->status_dir . '*') ) {

        my $flag_removal = 1;
        my $node_name = (split(/\//, $node_path))[-1];

        for my $raw_host ( @hosts ) {
            if ( $raw_host->{node_name} eq $node_name ) {
                $flag_removal = 0;
                last;
            }
        }
        
        if ( $flag_removal ) {
            $self->logger->debug("$node_path was flagged for removal");
            # This needs many constraints, but works as is
            unless( rmtree([$node_path]) ) {
                $self->logger->error("Attempted to remove $node_path, but failed!");
            } else {
                $self->logger->debug("Successfully removed $node_path");
            }
        }

    }
          

    $self->_set_hosts(\@hosts);
    $self->logger->debug("FINISHED processing hosts_dir from config");
}





=head2 start

=cut

sub start {

    my ( $self ) = @_;

    $self->logger->info( 'Starting.' );

    # Daemonized
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );

        my $pid_file = $self->config->get( '/config/@pid-file' )->[0];

        if ( !defined($pid_file) ) {
            $pid_file = "/var/run/simp_poller.pid";
        }

        $self->logger->debug("PID FILE: " . $pid_file);
        my $daemon = Proc::Daemon->new( pid_file => $pid_file );

        my $pid = $daemon->Init();

        # Jump out of parent
        return if ( $pid );

        $self->logger->debug( 'Created daemon process.' );

        # Change process name
        $0 = "simpPoller";

        # Figure out what user/group (if any) to change to
        my $user_name  = $self->run_user;
        my $group_name = $self->run_group;

        if ( defined($group_name) ) {

            my $gid = getgrnam($group_name);
            $self->_log_err_then_exit("Unable to get GID for group '$group_name'") if !defined($gid);

            $! = 0;
            setgid($gid);
            $self->_log_err_then_exit("Unable to set GID to $gid ($group_name)") if $! != 0;
        }

        if ( defined($user_name) ) {

            my $uid = getpwnam($user_name);
            $self->_log_err_then_exit("Unable to get UID for user '$user_name'") if !defined($uid);

            $! = 0;
            setuid($uid);
            $self->_log_err_then_exit("Unable to set UID to $uid ($user_name)") if $! != 0;
        }

    # Not Daemonized
    } else {
        $self->logger->debug( 'Running in foreground.' );
    }

    $self->logger->debug( 'Setting up signal handlers.' );

    # Setup signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        $self->logger->info( 'Received SIG HUP.' );
        $self->_set_do_reload(1);
        $self->stop();
        # Create and store the host portion of the config
    };

    while (1) {

        $self->logger->info( 'Main loop, running process_hosts_config');
        $self->_process_hosts_config();

        $self->logger->info( 'Main loop, running create_workers');
        $self->_create_workers();

        last if (! $self->do_reload);
        $self->_set_do_reload(0);
    }

    return 1;
}


sub _log_err_then_exit {

    my $self = shift;
    my $msg  = shift;

    $self->logger->error($msg);
    warn "$msg\n";
    exit 1;
}


=head2 stop

=cut

sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    # Kill children, then wait for them to finish exiting
    my $res = kill( 'TERM', @pids );
}


# End of multprocess boilerplate
sub _create_workers {

    my ( $self ) = @_;
    $self->logger->debug("BEGIN creating workers");

    # Get the set of active groups
    my $groups  = $self->config->get( "/config/group" );

    # For each host, one worker handles the host variables.
    # This hash keeps track of whether a host has had a worker assigned for that.
    my %var_worker;

    my $total_workers = 0;
    foreach my $group ( @$groups ) {
        # Ignore the group if it isnt active
        next if( $group->{'active'} == 0 );
        $total_workers += $group->{'workers'};
    }

    my $forker = Parallel::ForkManager->new( $total_workers );

    # Create workers for each group
    foreach my $group ( @$groups ) {
        # Ignore the group if it isnt active
        next if($group->{'active'} == 0);
        $self->logger->debug("Creating worker for active group: " . $group->{"name"});

        my $name            = $group->{"name"};
        my $workers         = $group->{'workers'};
        my $poll_interval   = $group->{'interval'};
        my $retention       = $group->{'retention'};
        my $snmp_timeout    = $group->{'snmp_timeout'};
        my $max_reps        = $group->{'max_reps'};

        # Get the set of OIDS for this group
        my @oids;
        foreach my $line ( @{$group->{'mib'}} ) {
            push( @oids, $line->{'oid'} );
            $self->logger->debug("OID Pushed: " . $line->{"oid"});
        }

        my %hostsByWorker;
        my %varsByWorker;
        my $idx=0;
        # Get the set of hosts that belong to this group
        my $id= $group->{'name'};

        my @hosts;

        foreach my $host ( @{$self->hosts} ) {
            my $groups = $host->{'group'};
            foreach my $group (keys %$groups) {
                if ($group eq $id) {
                    # Match add the host to the host list
                    push(@hosts,$host);
                    # No double-pushing:
                    last;
                }
            }
        }

        # Split hosts between workers
        foreach my $host (@hosts) {

            next if ( !defined($host->{'node_name'}) );

            push(@{$hostsByWorker{$idx}},$host);

            if ( !$var_worker{$host->{'node_name'}} ) {
                $var_worker{$host->{'node_name'}} = 1;
                $varsByWorker{$idx}{$host->{'node_name'}} = 1;
            }

            $idx++;

            if($idx>=$workers) { $idx = 0; }
        }

        $self->logger->info( "Creating $workers child processes for group: $name" );

        # Keep track of children pids
        $forker->run_on_finish( sub {
            my ( $pid ) = @_;
            $self->logger->error( "Child worker process $pid has died." );
        });


        # Create workers
        for (my $worker_id=0; $worker_id<$workers; $worker_id++) {

            my $pid = $forker->start();

            # We're still in the parent if so
            if ($pid) {
                $self->logger->debug( "Child worker process $pid created." );
                push( @{$self->children}, $pid );
                next;
            }

            # Create worker in this process
            my $worker = GRNOC::Simp::Poller::Worker->new(
                instance        => $worker_id,
                group_name      => $name,
                config          => $self->config,
                oids            => \@oids,
                hosts           => $hostsByWorker{$worker_id},
                poll_interval   => $poll_interval,
                retention       => $retention,
                logger          => $self->logger,
                status_dir      => $self->status_dir,
                max_reps        => $max_reps,
                snmp_timeout    => $snmp_timeout,
                var_hosts       => $varsByWorker{$worker_id} || {}
            );

            # This should only return if we tell it to stop via TERM signal etc.
            $worker->start();

            $self->logger->info("worker->start has finished for $$");

            # Exit child process
            $forker->finish();
        }
    }

    $self->logger->info( 'Waiting for all child worker processes to exit.' );

    $forker->wait_all_children();

    $self->_set_children( [] );

    $self->logger->info( 'All child workers have exited.' );

    $self->logger->debug("FINISHED creating workers");
}

1;
