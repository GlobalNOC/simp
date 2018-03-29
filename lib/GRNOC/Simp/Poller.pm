package GRNOC::Simp::Poller;

use strict;
use warnings;
use Data::Dumper;
use Moo;
use Types::Standard qw( Str Bool );
use List::Util qw( sum );

use Parallel::ForkManager;
use Proc::Daemon;
use POSIX qw( setuid setgid );

use GRNOC::Log;

our $VERSION = '1.0.6';

use GRNOC::Simp::Poller::Worker;
use GRNOC::Simp::Poller::Config;

### required attributes ###

=head2 public attributes

=over 12

=item config_file

=item hosts_file

=item logging_file

=item daemonize

=item run_user

=item run_group

=back

=cut

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has hosts_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );

### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

has run_user => ( is => 'ro',
                  required => 0 );

has run_group => ( is => 'ro',
                   required => 0 );

### private attributes ###

=head2 private_attributes

=over 12

=item config

=item logger

=item children

=back

=cut

has config => ( is => 'rwp' );

has logger => ( is => 'rwp' );

has children => ( is => 'rwp',
                  default => sub { [] } );

=head2 BUILD

=cut

sub BUILD {

    my ( $self ) = @_;

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file, watch => 120 );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    my $config = GRNOC::Simp::Poller::Config::build_config(
        config_file => $self->config_file,
        hosts_dir   => $self->hosts_file,
    );

    $self->_set_config( $config );

    return $self;
}

=head2 start

=cut


sub start {

    my ( $self ) = @_;

    $self->logger->info( 'Starting.' );

    $self->logger->debug( 'Setting up signal handlers.' );

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( 'Received SIG TERM.' );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( 'Received SIG HUP.' );
        $self->stop();
        # create and store the host portion of the config
        $self->_process_hosts_config();
        $self->_create_workers();
    };

    # need to daemonize
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );

        my $pid_file = $self->config->{'global'}{'pid_file'};

        $self->logger->debug("PID FILE: " . $pid_file);
        my $daemon = Proc::Daemon->new( pid_file => $pid_file );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {

            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "simpPoller";

            # figure out what user/group (if any) to change to
            my $user_name  = $self->run_user;
            my $group_name = $self->run_group;

            if (defined($group_name)) {
                my $gid = getgrnam($group_name);
                $self->_log_err_then_exit("Unable to get GID for group '$group_name'") if !defined($gid);

                $! = 0;
                setgid($gid);
                $self->_log_err_then_exit("Unable to set GID to $gid ($group_name)") if $! != 0;
            }

            if (defined($user_name)) {
                my $uid = getpwnam($user_name);
                $self->_log_err_then_exit("Unable to get UID for user '$user_name'") if !defined($uid);

                $! = 0;
                setuid($uid);
                $self->_log_err_then_exit("Unable to set UID to $uid ($user_name)") if $! != 0;
            }

            $self->_create_workers();
        }
    }

    # dont need to daemonize
    else {

        $self->logger->debug( 'Running in foreground.' );

        $self->_create_workers();
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

    my $res = kill( 'TERM', @pids );

    $self->_set_children([]);


}

#-------- end of multprocess boilerplate
sub _create_workers {

    my ( $self ) = @_;

    #--- get the set of active groups
    my %groups = %{$self->config->{'groups'}};

    # For each host, one worker handles the host variables.
    # This hash keeps track of whether a host has had a worker assigned for that.
    my %var_worker;

    my $total_workers = sum ( map { scalar @{$groups{$_}{'workers'}} } (keys %groups) );

    my $forker = Parallel::ForkManager->new( $total_workers + 2);

    #--- create workers for each active group
    foreach my $group (values %groups){

      my $name    = $group->{"name"};
      my @worker_names = @{$group->{'workers'}};
      my $num_workers  = scalar(@worker_names);

      my $poll_interval = $group->{'interval'};
      my $retention     = $group->{'retention'};
      my $snmp_timeout  = $group->{'snmp_timeout'};
      my $max_reps      = $group->{'max_reps'};

      #--- get the set of OIDS for this group
      my @oids = @{$group->{'mib'}};

      my %hostsByWorker = map { $_ => [] } @worker_names;
      my %varsByWorker  = map { $_ => {} } @worker_names;
      my $idx = 0;

      #--- get the set of hosts that belong to this group
      my $hosts = $self->config->{'host_groups'}{$name};
      my @hosts;
      @hosts = @$hosts if defined($hosts);

      #--- split hosts between workers
      foreach my $host (@hosts){
        push(@{$hostsByWorker{$worker_names[$idx]}},$host);
        if(!$var_worker{$host->{'node_name'}}){
            $var_worker{$host->{'node_name'}} = 1;
            $varsByWorker{$worker_names[$idx]}{$host->{'node_name'}} = 1;
        }
        $idx++;
        if($idx>=$num_workers) { $idx = 0; }
      }

      $self->logger->info( "Creating $num_workers child processes for group: $name" );

      # keep track of children pids
      $forker->run_on_start( sub {

        my ( $pid ) = @_;

        $self->logger->debug( "Child worker process $pid created." );

        push( @{$self->children}, $pid );
      } );

      # keep track of children pids
      $forker->run_on_finish( sub {

          my ( $pid ) = @_;

          $self->logger->error( "Child worker process $pid has died." );


                              } );


      # create workers
      foreach my $worker_name (@worker_names) {
          $forker->start() and next;

          # create worker in this process
          my $worker = GRNOC::Simp::Poller::Worker->new( worker_name   => $worker_name,
                                                         group_name    => $name,
                                                         global_config => $self->config->{'global'},
                                                         oids          => \@oids,
                                                         hosts         => $hostsByWorker{$worker_name},
                                                         poll_interval => $poll_interval,
                                                         retention     => $retention,
                                                         logger        => $self->logger,
                                                         max_reps      => $max_reps,
                                                         snmp_timeout  => $snmp_timeout,
                                                         var_hosts     => $varsByWorker{$worker_name},
                                                         monitoring_retention => $group{'monitoring_retention'},
              );

        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();

        # exit child process
        $forker->finish();
      }

    }

    $self->logger->debug( 'Waiting for all child worker processes to exit.' );

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children( [] );

    $self->logger->debug( 'All child workers have exited.' );
}

1;
