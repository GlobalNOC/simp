package GRNOC::Simp::Comp;

use strict;
use warnings;
### REQUIRED IMPORTS ###
use Moo;
use Types::Standard qw( Str Bool );
use Parallel::ForkManager;
use Proc::Daemon;
use POSIX qw( setuid setgid );
use Data::Dumper;

our $VERSION='1.0.7';
use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Simp::Comp::Worker;

### REQUIRED ATTRIBUTES ###
=head2 public attributes

=over 12

=item config_file

=item logging_file

=item composites_dir

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

has composites_dir => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

has config_xsd => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

has composite_xsd => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

### OPTIONAL ATTRIBUTES ###

has daemonize => ( 
    is          => 'ro',
    isa         => Bool,
    default     => 1 
);

has run_user => ( 
    is          => 'ro',
    required    => 0 
);

has run_group => ( 
    is          => 'ro',
    required    => 0 
);

### PRIVATE ATTRIBUTES ###
=head2 private attributes

=over 12

=item config

=item logger

=item composites

=item children

=back

=cut

has config => ( 
    is => 'rwp' 
);

has logger => ( 
    is => 'rwp' 
);

has composites => (
    is => 'rwp',
    default => sub { {} }
);

has children => ( 
    is      => 'rwp',
    default => sub { [] } 
);


=head2 _validate_config
=cut
sub _validate_config {
    
    my $self = shift;
    my $file = shift;
    my $conf = shift;
    my $xsd  = shift;

    my $validation_code = $conf->validate($xsd);
    
    if ($validation_code == 1) {
        $self->logger->debug("Successfully validated $file");
        return;
    }
    elsif ($validation_code == 0) {
        $self->logger->error("ERROR: Failed to validate $file!\n" . $conf->{error}->{backtrace});
    }
    else {
        $self->log_and_exit("ERROR: XML schema in $xsd is invalid!\n" . $conf->{error}->{backtrace});
    }
    exit(1);
}


=head2 BUILD
    Builds the simp_comp object with config, composites, and logger
=cut
sub BUILD {

    my ($self) = @_;

    # Create and store the logger object
    my $grnoc_log = GRNOC::Log->new( 
        config => $self->logging_file, 
        watch => 120 
    );
    my $logger = GRNOC::Log->get_logger();

    $self->_set_logger( $logger );

    # Create, validate, and store the main config object
    my $config = GRNOC::Config->new( 
        config_file => $self->config_file,
        force_array => 0 
    );
    $self->_validate_config($self->config_file, $config, $self->config_xsd);
 
    $self->_set_config( $config );

    $self->_process_composites();

    return $self;
}


=head2 _process_composites
    Reads the composites dir and creates a hash of XPath objects for each composite
=cut
sub _process_composites {
    
    my $self = shift;

    $self->logger->info("Reading composites from " . $self->composites_dir);

    # Read XML files from the composites dir
    opendir(my $dir, $self->composites_dir) or $self->_log_and_exit("Could not open $self->composites_dir!");
    my @files = grep {$_ =~ /\.xml$/} readdir($dir);
    close($dir);

    # Create a hash of composite names mapped to their XPath object
    my %composites;
    foreach my $file (@files) {

        my $composite_name = substr($file, 0, -4);
        
        my $composite = GRNOC::Config->new(
            config_file => $self->composites_dir . $file,
            force_array => 1
        );

        $self->_validate_config($file, $composite, $self->composite_xsd);
            
        $composites{$composite_name} = $composite;

    }
    $self->_set_composites(\%composites);
    $self->logger->info("Composite definitions read successfully: " . scalar(keys %composites));
}


=head2 start
    Starts the simp_comp process and creates all of its workers
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

    # Daemonized
    if ( $self->daemonize ) {

        $self->logger->debug( 'Daemonizing.' );

        my $pid_file = $self->config->get( '/config/@pid-file' );
        if( !defined($pid_file) ) {
            $pid_file = "/var/run/simp_comp.pid";
        }

        $self->logger->debug("PID FILE: " . $pid_file);
        my $daemon = Proc::Daemon->new( pid_file => $pid_file,
            child_STDOUT => '/tmp/simp_comp.out',
            child_STDERR => '/tmp/simp_comp.err'
         );
        my $pid = $daemon->Init();

        # in child/daemon process
        if ( !$pid ) {

            $self->logger->debug( 'Created daemon process.' );

            # change process name
            $0 = "simp_comp";

            # figure out what user/group (if any) to change to
            my $user_name  = $self->run_user;
            my $group_name = $self->run_group;

            if ( defined($group_name) ) {
                my $gid = getgrnam($group_name);
                $self->_log_and_exit("Unable to get GID for group '$group_name'") if !defined($gid);
                $! = 0;
                setgid($gid);
                $self->_log_and_exit("Unable to set GID to $gid ($group_name)") if $! != 0;
            }

            if ( defined($user_name) ) {
                my $uid = getpwnam($user_name);
                $self->_log_and_exit("Unable to get UID for user '$user_name'") if !defined($uid);
                $! = 0;
                setuid($uid);
                $self->_log_and_exit("Unable to set UID to $uid ($user_name)") if $! != 0;
            }

            $self->_create_workers();
        }

    # Run in the foreground
    } else {

        $self->logger->debug( 'Running in foreground.' );

        #-- when in fg just act as a working directly with no sub processes so we can nytprof 
        my $worker = GRNOC::Simp::CompData::Worker->new(
            config     => $self->config,
            logger     => $self->logger,
            composites => $self->composites,
            worker_id  => 'foreground'
        );
	
        # this should only return if we tell it to stop via TERM signal etc.
        $worker->start();
    }

    return 1;
}


=head2 _log_and_exit
    Logs an error and then exits the script
=cut
sub _log_and_exit {
    my $self = shift;
    my $msg  = shift;
    $self->logger->error($msg);
    warn "$msg\n";
    exit 1;
}


=head2 stop
    Kills all simp_comp processes
=cut
sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    return kill( 'TERM', @pids );
}


=head2 _create_workers
    Creates all simp_comp workers
=cut
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
    });

    # create high res workers
    for (my $worker_id=0; $worker_id<$workers;$worker_id++) {
        
        $forker->start() and next;


	    # create worker in this process
	    my $worker = GRNOC::Simp::CompData::Worker->new( 
            config     => $self->config,
            logger     => $self->logger,
            composites => $self->composites,
            worker_id  => $worker_id
        );
	
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
