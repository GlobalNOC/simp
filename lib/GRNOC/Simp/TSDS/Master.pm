package GRNOC::Simp::TSDS::Master;

use strict;
use warnings;

use Log::Log4perl;
use Moo;
use Types::Standard qw(Str Bool);
use Proc::Daemon;
use AnyEvent::Subprocess;
use POSIX qw(setuid setgid);
use Data::Dumper; 

use GRNOC::Config;
use GRNOC::RabbitMQ::Client;
use GRNOC::Simp::TSDS::Worker;

has config_file => (is => 'ro', isa => Str, required => 1);
has pidfile => (is => 'ro', isa => Str, required => 1);
has daemonize => (is => 'ro', isa => Bool, required => 1);
has run_user => (is => 'ro', required => 0);
has run_group => (is => 'ro', required => 0);

has logger => (is => 'rwp');
has simp_config => (is => 'rwp');
has tsds_config => (is => 'rwp');
has collections => (is => 'rwp', default => sub { [] });
has worker_client => (is => 'rwp');
has children => (is => 'rwp', default => sub {[]});
has hup => (is => 'rwp', default => 0);

my $running;

sub BUILD {
    my $self = shift;
    
    $self->_set_logger(Log::Log4perl->get_logger('GRNOC.Simp.TSDS.Master'));

    return $self;
}

#
# Start up the master
#
sub start {
    my ($self) = @_;
    
    $self->logger->info('Starting.');
    $self->logger->debug('Setting up signal handlers.');


    # Daemonize if needed
    if ($self->daemonize) {
	$self->logger->debug('Daemonizing.');

	my $daemon = Proc::Daemon->new( pid_file => $self->pidfile,
				        child_STDOUT => '/tmp/oess-vlan-collector.out',
				        child_STDERR => '/tmp/oess-vlan-collector.err');
	my $pid = $daemon->Init();

	if ($pid) {
	    sleep 1;
	    die 'Spawning child process failed' if !$daemon->Status();
	    exit(0);
	}
    }

    warn "Child is alive!!\n";

    # If requested, change to different user and group
    if (defined($self->run_group)) {
	my $run_group = $self->run_group;
	my $gid = getpwnam($self->run_group);
	die "Unable to get GID for group '$run_group'\n" if !defined($gid);
	$! = 0;
	setgid($gid);
	die "Unable to set GID to '$run_group' ($gid): $!\n" if $! != 0;
    }

    if (defined($self->run_user)) {
	my $run_user = $self->run_user;
	my $uid = getpwnam($run_user);
	die "Unable to get UID for user '$run_user'\n" if !defined($uid);
	$! = 0;
	setuid($uid);
	die "Unable to set UID to '$run_user' ($uid): $!\n" if $! != 0;
    }

    # Only run once unless HUP gets set, then reload and go again
    while (1) {
        $self->_set_hup(0);
	$self->_load_config();
	$self->_create_workers();
	last unless $self->hup;
    }

    $self->logger->info("Master terminating");
}

#
# Load config and set up Master object
#
sub _load_config {
    my ($self) = @_;

    $self->logger->info("Reading configuration from " . $self->config_file);

    my $conf = GRNOC::Config->new(config_file => $self->config_file,
				       force_array => 1);

    $self->_set_simp_config($conf->get('/config/simp')->[0]);

    $self->_set_tsds_config($conf->get('/config/tsds')->[0]);
    
    $self->_set_collections($conf->get('/config/collection'));

    # Sanity-check some of the collection parameters
    foreach my $collection (@{$self->collections}) {
        my $should_die = 0;

        if (!defined($collection->{'tsds_type'}) || ($collection->{'tsds_type'} eq '')) {
            $self->logger->error('No or invalid TSDS measurement type defined for a collection! Exiting.');
            $should_die = 1;
        }
        if (!defined($collection->{'interval'})) {
            $self->logger->error('Interval not defined for a collection! Exiting.');
            $should_die = 1;
        } elsif ($collection->{'interval'} < 1) {
            $self->logger->error("Invalid interval '$collection->{'interval'}'! Exiting.");
            $should_die = 1;
        }
        if (!defined($collection->{'composite-name'})) {
            $self->logger->error('Composite for a collection not defined! Exiting.');
            $should_die = 1;
        }
        if ($collection->{'filter_value'} xor $collection->{'filter_name'}) {
            $self->logger->error('If filtering, both filter_name and filter_value must be specified! Exiting.');
            $should_die = 1;
        }
        if (!defined($collection->{'workers'})) {
            $self->logger->error('Number of workers not defined for a collection! Exiting.');
            $should_die = 1;
        } elsif ($collection->{'workers'} < 1) {
            $self->logger->error("Invalid number of workers '$collection->{'workers'}'! Exiting.");
            $should_die = 1;
        }

        die if $should_die;

        $collection->{'host'} = [] if !defined($collection->{'host'});
    }

    $self->_set_worker_client(undef);
}

#
# _create_workers creates and starts a number of Workers. When this
# process receives TERM, INT, or HUP the Workers are told exactly once
# to quit. Once all workers have joined this function returns.
#
sub _create_workers {
    my $self = shift;

    # Create separate groups of workers for each collection
    foreach my $collection (@{$self->collections}) {
        $self->_create_workers_for_one_collection($collection);
    }

    $running = AnyEvent->condvar;

    $SIG{'TERM'} = sub {
	$self->logger->info('Received SIGTERM.');
	foreach my $worker (@{$self->children}){
	    $worker->kill();
	}
	exit;
    };

    $SIG{'INT'} = sub {
        $self->logger->info('Received SIGINT.');
        foreach my $worker (@{$self->children}){
            $worker->kill();
        }
        exit;
    };

    $SIG{'HUP'} = sub {
	$self->logger->info('Received SIGHUP.');
	while(my $worker = pop @{$self->children}){
            $worker->kill();
        }
	
	$self->_set_hup(1);	
	$running->send;
    };

    $running->recv;
}

sub _create_workers_for_one_collection {
    my ($self, $collection) = @_;

    my %hosts_by_worker;
    my $idx = 0;

    # Divide up hosts in config among number of workers defined in config
    foreach my $host (@{$collection->{'host'}}) {
        next if !defined($host) || (ref($host) ne '');
	push(@{$hosts_by_worker{$idx}}, $host);
	$idx++;
	if ($idx >= $collection->{'workers'}) {
	    $idx = 0;
	}
    }

    # Spawn workers
    foreach my $worker_id (keys %hosts_by_worker) {
	my $worker_name = "$collection->{'composite-name'}_$worker_id";

	$self->_create_worker( name       => $worker_name,
                               collection => $collection,
			       hosts      => $hosts_by_worker{$worker_id});
    }
}

sub _create_worker{
    my $self = shift;
    my %params = @_;

    my $collection = $params{'collection'};

    my $init_proc = AnyEvent::Subprocess->new(
        on_completion => sub {
            $self->logger->error("Child " . $params{'name'} . " has died");
            #do something to restart
            #pop the worker off the queue
            $self->_create_worker( %params );
        },
        code => sub {
            use GRNOC::Log;
            use GRNOC::Simp::TSDS::Worker;

            my $required_vals = $collection->{'required_values'};
            $required_vals = '' if !defined($required_vals);

            $self->logger->info("Creating Collector for " . $params{'name'});
            my $worker = GRNOC::Simp::TSDS::Worker->new(
                worker_name => $params{'name'},
                logger => $self->logger,
                composite_name => $collection->{'composite-name'},
                hosts => $params{'hosts'},
                simp_config => $self->simp_config,
                tsds_config => $self->tsds_config,
                tsds_type => $collection->{'tsds_type'},
                interval => $collection->{'interval'},
                filter_name => $collection->{'filter_name'},
                filter_value => $collection->{'filter_value'},
                required_value_fields => [ split(',', $required_vals) ],
            );
            $worker->run();
        }
    );

    my $proc = $init_proc->run();
    push(@{$self->children}, $proc);
}
    1;
