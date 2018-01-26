package GRNOC::Simp::CompData;

use strict;
use warnings;
use Moo;
use Types::Standard qw( Str Bool );

use Parallel::ForkManager;
use Proc::Daemon;

use GRNOC::Config;
use GRNOC::Log;

use GRNOC::Simp::CompData::Worker;

### required attributes ###
=head2 public attributes

=over 12

=item config_file

=item logging_file

=item daemonize

=back

=cut

has config_file => ( is => 'ro',
                     isa => Str,
                     required => 1 );

has logging_file => ( is => 'ro',
                      isa => Str,
                      required => 1 );

### optional attributes ###

has daemonize => ( is => 'ro',
                   isa => Bool,
                   default => 1 );

### private attributes ###
=head2 private attributes

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

sub BUILD{
    my ($self) = @_;

    # create and store logger object
    my $grnoc_log = GRNOC::Log->new( config => $self->logging_file, watch => 120 );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # create and store config object
    my $config = GRNOC::Config->new( config_file => $self->config_file,
                                     force_array => 0 );

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
    };

    # need to daemonize
    if ( $self->daemonize ) {


        $self->logger->debug( 'Daemonizing.' );

        my $pid_file = $self->config->get( '/config/@pid-file' );
        if(!defined($pid_file)){
            $pid_file = "/var/run/simp_comp.pid";
        }

        $self->logger->debug("PID FILE: " . $pid_file);
        my $daemon = Proc::Daemon->new( pid_file => $pid_file );

        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {

            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "CompData";

            my $uid = getpwnam('simp');
            $> = $uid;

            $self->_create_workers();
        }
    }

    # dont need to daemonize
    else {

        $self->logger->debug( 'Running in foreground.' );

        #-- when in fg just act as a working directly with no sub processes so we can nytprof 
        my $worker = GRNOC::Simp::CompData::Worker->new( config    => $self->config,
						     logger    => $self->logger,
						     worker_id => 13 );
	
        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();
    }

    return 1;
}

=head2 stop

=cut

sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    return kill( 'TERM', @pids );
}

#-------- end of multprocess boilerplate
sub _create_workers {

    my ( $self ) = @_;

    my $workers = $self->config->get( '/config/@workers' );

    $self->logger->info( "Creating $workers child worker processes." );

    my $forker = Parallel::ForkManager->new( $workers );

    # keep track of children pids
    $forker->run_on_start( sub {

        my ( $pid ) = @_;
        $self->logger->debug( "Child worker process $pid created." );
        push( @{$self->children}, $pid );
                           } );
    # create high res workers
    for (my $worker_id=0; $worker_id<$workers;$worker_id++) {
        
        $forker->start() and next;


	# create worker in this process
	my $worker = GRNOC::Simp::CompData::Worker->new( config    => $self->config,
						     logger    => $self->logger,
						     worker_id => $worker_id );
	
	# this should only return if we tell it to stop via TERM signal etc.
	$worker->start();
	
	# exit child process
        $forker->finish();
    }

    $self->logger->debug( 'Waiting for all child worker processes to exit.' );

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children( [] );

    $self->logger->debug( 'All child workers have exited.' );
}

1;
