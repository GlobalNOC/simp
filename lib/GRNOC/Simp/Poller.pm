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

our $VERSION = '1.3.0';

use GRNOC::Simp::Poller::Worker;

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

has groups_dir => (
    is => 'ro',
    isa => Str,
    required => 1
);

has validation_dir => (
    is       => 'ro',
    isa      => Str,
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

=item logger

=item hosts

=item groups

=item total_workers

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

has groups => (
    is => 'rwp'
);

has total_workers => (
    is => 'rwp',
    default => 0
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
    Builds the simp_poller object and sets its parameters.
=cut
sub BUILD {

    my ( $self ) = @_;

    # Create and store logger object
    my $grnoc_log = GRNOC::Log->new(
        config => $self->logging_file, 
        watch => 120 
    );
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger( $logger );

    # Create the main config object
    my $config = GRNOC::Config->new( 
        config_file => $self->config_file,
        force_array => 1
    );

    # Get the validation file for config.xml
    my $xsd = $self->validation_dir . 'config.xsd';
    
    # Validate the config
#    my $validation_code = $config->validate($xsd);

    # Use the validation code to log the outcome and exit if any errors occur
#    if ($validation_code == 1) {
#        $self->logger->debug("Successfully validated $self->config_file");
#    }
#    else { 
#        if ($validation_code == 0) {
#            $self->logger->error("ERROR: Failed to validate $self->config_file!\n" . $config->{error}->{backtrace});
#        }
#        else {
#            $self->logger->error("ERROR: XML schema in $xsd is invalid!\n" . $config->{error}->{backtrace});
#        }
#        exit(1);
#    }

    # Validate the main config useing the xsd file for it
    $self->_validate_config($self->config_file, $config, $xsd);

    # Set the config if it validated
    $self->_set_config( $config );

    # Set status_dir to path in the configs, if defined and is a dir
    my $status_path = $self->config->get('/config/status');
    my $status_dir  = $status_path ? $status_path->[0]->{dir} : undef;

    if ( defined $status_dir && -d $status_dir) {
        if ( substr($status_dir, -1) ne '/') {
            $status_dir .= '/';
            $self->logger->debug("The path for status_dir didn't include a trailing slash, added it: $status_dir");
        }
        $self->_set_status_dir( $status_dir );
        $self->logger->debug("Found poller_status dir in config, using: " . $self->status_dir);

    # Use default if not defined in configs, or invalid
    } else {
        $self->logger->debug("No valid poller_status dir defined in config, using: " . $self->status_dir);
    }

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
        $self->logger->debug("Successfully validated $file");
        return 1;
    }
    else {
        if ($validation_code == 0) {
            $self->logger->error("ERROR: Failed to validate $file!\n" . $config->{error}->{backtrace});
        }
        else {
            $self->logger->error("ERROR: XML schema in $xsd is invalid!\n" . $config->{error}->{backtrace});
        }
        exit(1);
    }
}


=head2 _get_config_objects
    Retrieves the XPath objects of a target from every XML file in a config dir.
    Returns the objects in an array reference.
=cut
sub _get_config_objects {

    my $self       = shift;
    my $target_obj = shift;
    my $target_dir = shift;
    my $xsd        = shift;

    # The final hash of config objects to return
    my %config_objects;

    $self->logger->debug("Getting $target_obj XPath objects from $target_dir");

    # Load all files in the target_dir into an array
    opendir(my $dir, $target_dir);
    my @files = readdir $dir;
    closedir($dir);

    $self->logger->debug("Files found:\n" . Dumper(\@files));

    # Check every file in the target dir
    foreach my $file (@files) {

        # Only process valid XML files
        next unless $file =~ /\.xml$/;

        # Make an XPath object from the file
        my $config = GRNOC::Config->new(
            config_file => $target_dir . $file,
            force_array => 1
        );

        # Validate the config using the main config xsd file
        $self->_validate_config($file, $config, $xsd);

        # Push each targeted XPath object found in the file into the final array
        foreach my $object ( @{$config->get($target_obj)} ) {

            # Check if the object has been set to inactive
            unless ($object->{active} and $object->{active} == 0) {

                # Use the object name as the key to the object
                if ( exists $object->{name} ) {
                    $config_objects{$object->{name}} = $object;
                    delete $object->{name};
                }
                # Use the config file name as the key to the object
                else {
                    $config_objects{substr($file, 0, -4)} = $object;
                }
            }   
            else {
                $self->logger->debug('Skipping inactive object');
            }
        }
    }

    $self->logger->debug("Got a total of " . scalar(keys %config_objects) . " $target_obj objects");
    return \%config_objects;
}


=head2 _process_hosts_config
    Process the host configs for their polling groups.
    Create and remove status dirs based upon which hosts are found.
    Set the hosts for the poller objects.
=cut
sub _process_hosts_config {

    my $self = shift;
    $self->logger->debug("BEGIN processing hosts_dir from config");

    my $hosts = $self->_get_config_objects('/config/host', $self->hosts_dir, $self->validation_dir.'hosts.xsd');
    $self->logger->debug(Dumper($hosts));

    foreach my $host_name (keys %$hosts) {
        # Check if status dir for each host exists, or create it.
        my $mon_dir = $self->status_dir . $host_name . '/';
        unless ( -e $mon_dir || system("mkdir -m 0755 -p $mon_dir") == 0 ) {
            $self->logger->error("Could not find or create dir for monitoring data: $mon_dir");
        } else {
            $self->logger->debug("Found or created status dir for $host_name successfully");
        }
    }
   
    # Once status dirs for configured hosts have been made...
    # Remove any dirs for hosts that are not included in any hosts.d file
    # Remove any status files in a valid host dir for groups the host doesn't have configured
    foreach my $host_dir ( glob($self->status_dir . '*') ) {

        my $dir_name = (split(/\//, $host_dir))[-1];

        # Remove the dir unless the dir name exists in the hosts hash
        unless ( exists $hosts->{$dir_name} ) {
        
            $self->logger->debug("$host_dir was flagged for removal");

            # This needs constraints, but works as is
            unless( rmtree([$host_dir]) ) {
                $self->logger->error("Attempted to remove $host_dir, but failed!");
            } else {
                $self->logger->debug("Successfully removed $host_dir");
            }
        }
        # The hosts' status dir exists, make sure there are only status files for active groups
        else {
            foreach my $status_file ( glob($host_dir . '/*') ) {

                # Get the filename
                my $file  = (split(/\//, $status_file))[-1];

                # Get the polling group's name 
                $file =~ /^(.+)_status\.json$/;

                # Unless the host is configured for the group, delete the status file
                unless (exists $hosts->{$dir_name}{group}{$1}) {
                    unlink $status_file;
                    $self->logger->debug("Removed status file $file in $host_dir");
                }
            }
        }

    }
          
    $self->_set_hosts($hosts);
    $self->logger->debug("FINISHED processing hosts_dir from config");
}


=head2 _process_groups_config
    Process the group configs for their polling oids.
    Set the groups for the poller object.
=cut
sub _process_groups_config {

    my $self = shift;
    $self->logger->debug("BEGIN processing groups_dir from config");

    # Get the default results-per-request size (max_repetitions) from the poller config. (Default to 15)
    my $request_size = $self->config->get('/config/request_size');
    my $num_results  = $request_size ? $request_size->[0]->{results} : 15;

    # Get the group objects from the files in groups.d
    my $groups = $self->_get_config_objects('/group', $self->groups_dir, $self->validation_dir.'group.xsd');

    # Get the total number of workers to fork and set the group name and any defaults
    my $total_workers = 0;
    foreach my $group_name (keys %$groups) {

        my $group = $groups->{$group_name};
       
        # Set the oids for the group from the mib elements 
        $group->{oids} = [];
        foreach my $mib (@{$group->{mib}}) {
            push($group->{oids}, $mib);
        }
        delete $group->{mib};

        $self->logger->debug("Optional settings for group $group_name");
    
        # Set retention time to 5x the polling interval
        unless ($group->{retention}) {
            $group->{retention} = $group->{interval} * 5;
            $self->logger->debug("Retention Period: $group->{retention} (default)");
        } else {
            $self->logger->debug("Retention Period: $group->{retention} (specified)");
        }

        # Set the packet results size (max_repetitions) or default to size from poller config
        unless ($group->{request_size}) {
            $group->{request_size} = $num_results;
            $self->logger->debug("Request Size: $group->{request_size} results (default)\n\n");
        } else {
            $self->logger->debug("Request Size: $group->{request_size} results (specified)\n\n");
        }

        # Set the hosts that belong to the polling group
        $group->{hosts} = ();
        foreach my $host_name ( keys %{$self->hosts} ) {

            my $host = $self->hosts->{$host_name};
            
            foreach my $host_group ( keys %{$self->hosts->{$host_name}->{group}} ) {
                if ($host_group eq $group_name) {
                    push(@{$group->{hosts}}, $host_name);
                }
            }
        }

        # Add the groups' workers to the total number of forks to create
        $total_workers += $group->{workers};
    }

    # Set the number of forks and then the groups for the poller object
    $self->_set_total_workers($total_workers);
    $self->_set_groups($groups);
    
    $self->logger->debug(Dumper($groups));

    $self->logger->debug('FINISHED processing groups_dir from config');

}


=head2 start
    Starts all of the simp_poller processes
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
        $0 = "simp_poller [master]";

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

        $self->logger->info('Main loop, running process_groups_config');
        $self->_process_groups_config();

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
    Stops all of the simp_poller processes.
=cut
sub stop {

    my ( $self ) = @_;

    $self->logger->info( 'Stopping.' );

    my @pids = @{$self->children};

    $self->logger->debug( 'Stopping child worker processes ' . join( ' ', @pids ) . '.' );

    # Kill children, then wait for them to finish exiting
    my $res = kill( 'TERM', @pids );
}


=head2 _create_workers
    Creates the forked worker processes for each polling group.
    Delegates hosts to their polling groups' workers.
=cut
sub _create_workers {

    my ( $self ) = @_;

    $self->logger->debug("BEGIN creating workers");

    # Create a fork for each worker that is needed
    my $forker = Parallel::ForkManager->new( $self->total_workers );

    # Create workers for each group
    foreach my $group_name ( keys %{$self->groups} ) {

        my $group = $self->groups->{$group_name};

        $self->logger->debug("Creating worker for group: " . $group_name);

        # Split hosts into worker groups
        my %worker_hosts;
        my $i = 0;
        foreach my $host_name (@{$group->{hosts}}) {
            
            next if ( !defined($host_name) );

            my $host = $self->hosts->{$host_name};

            $worker_hosts{$i}{$host_name} = $host;

            $i++;

            if ( $i >= $group->{workers} ) { $i = 0; }
        }

        $self->logger->debug(Dumper(\%worker_hosts));

        $self->logger->info( "Creating $group->{workers} child processes for group: $group_name" );

        # Keep track of children pids
        $forker->run_on_finish( sub {
            my ( $pid ) = @_;
            $self->logger->error( "Child worker process $pid ($group_name) has died." );
        });

        # Create workers
        for (my $worker_id=0; $worker_id < $group->{workers}; $worker_id++) {

            my $pid = $forker->start();

            # We're still in the parent if so
            if ($pid) {
                $self->logger->debug( "Child worker process $pid ($group_name) created." );
                push( @{$self->children}, $pid );
                next;
            }

            # Create worker in this process
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
                hosts        => $worker_hosts{$worker_id} || {}
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
