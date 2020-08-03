package GRNOC::Simp::Comp;

use strict;
use warnings;

### REQUIRED IMPORTS ###
use Data::Dumper;
use Moo;
use POSIX qw( setuid setgid );
use Parallel::ForkManager;
use Proc::Daemon;
use Types::Standard qw( Str Bool );

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::Simp::Comp::Worker;

our $VERSION = '1.4.3';

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

### PRIVATE ATTRIBUTES ###

=head2 private attributes

=over 12

=item config
=item rmq_config
=item logger
=item composites
=item children

=back

=cut

has config => (is => 'rwp');

has rmq_config => (is => 'rwp');

has logger => (is => 'rwp');

has composites => (
    is      => 'rwp',
    default => sub { {} }
);

has children => (
    is      => 'rwp',
    default => sub { [] }
);


=head2 BUILD
    Builds the simp_comp object with config, composites, and logger
=cut
sub BUILD {

    my ($self) = @_;

    $self->_make_logger();
    $self->_make_config();
    $self->_make_composites();

    return $self;
}


=head2 _validate_config
    Validates configuration against its XSD file
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
        $self->logger->error("ERROR: XML schema in $xsd is invalid!\n" . $conf->{error}->{backtrace});
    }

    exit(1);
}


=head2 _make_logger
    Creates the logger for Comp main and workers to use
=cut
sub _make_logger {

    my $self = shift;

    # Create a new GRNOC::Log object
    my $log = GRNOC::Log->new(
        config => $self->logging_file,
        watch  => 120
    );
    
    # Get and set the logger from its object
    my $logger = GRNOC::Log->get_logger();
    $self->_set_logger($logger);
}


=head2 _make_config
    Reads the main config, validates it, and sets the main config hash
    This also sets the RabbitMQ config hash
=cut
sub _make_config {

    my $self = shift;

    # Initialize the GRNOC::Config object
    my $config = GRNOC::Config->new(
        config_file => $self->config_file,
        force_array => 0
    );

    # Validate the config file
    $self->_validate_config($self->config_file, $config, $self->config_xsd);

    # Set the config for Comp main and workers
    $self->_set_config($config->get('/config'));

    if ($self->config) {
        # Set the RabbitMQ config for main and workers
        $self->_set_rmq_config($self->config->{'rabbitmq'});
    }
    else {
        # For testing, we set RabbitMQ to an empty hash
        $self->_set_rmq_config({});
    }
}

=head2 _make_composites
    Reads the composites dir and maps composite names to a hash of their parameters.
    Each composite is read, validated, and then processed for consistency.
    The hashes returned from GRNOC::Config->get() are not always consistent.
    This is why we process the configs here to be passed to workers as a referance.
=cut
sub _make_composites {

    my $self = shift;

    $self->logger->info("Reading composites from " . $self->composites_dir);

    # Read XML files from the composites dir
    my @files;
    opendir(my $dir, $self->composites_dir) or $self->_log_and_exit("Could not open " . $self->composites_dir . ": $!");
    @files = grep { $_ =~ /\.xml$/ } readdir($dir);
    close($dir);

    # Create a hash where composite names are mapped to a hash of their parameters
    my %composites;
    
    # Process each individual composite config
    for my $file (@files) {
    
        # Composite names are taken from their file name
        my $composite_name = substr($file, 0, -4);

        # Create a new GRNOC::Config object for the composite
        my $config = GRNOC::Config->new(
            config_file => $self->composites_dir . $file,
            force_array => 1
        );

        # Validate the composite's config file
        $self->_validate_config($file, $config, $self->composite_xsd);

        # Begin building the composite hash used by Workers from the config
        my %composite;
        
        # Get the hash of parameters
        my $parameters = $config->get('/composite')->[0];

        # Begin processing the given parameters for "variables"
        my $variables = $parameters->{'variables'}[0];

        # Process input variables
        if (exists $variables->{'input'}) {
            for my $input_name (keys %{$variables->{'input'}}) {
                $composite{'inputs'}{$input_name} = $variables->{'input'}{$input_name};
            }
        }
        else {
            $composite{'inputs'} = {};
        }

        # Process constants
        if (exists $variables->{'constant'}) {
            for my $const_name (keys %{$variables->{'constant'}}) {
                $composite{'constants'}{$const_name} = $variables->{'constant'}{$const_name}{'value'};
            }
        }
        else {
            $composite{'constants'} = {};
        }

        # Process scans
        if (exists $variables->{'scan'} && ref $variables->{'scan'} eq 'ARRAY') {

            for my $scan (@{$variables->{'scan'}}) {
                
                my $scan_name = $scan->{'poll_value'};
                
                $composite{'scans'}{$scan_name} = {
                    'oid'    => $scan->{'oid'},
                    'suffix' => $scan->{'oid_suffix'},
                    'map'    => $self->_map_oid($scan_name, $scan, 'scan')
                };
            }
        }

        # Begin processing the given parameters for "data"
        my $data = $parameters->{'data'}[0];

        my $metadata = exists $data->{'meta'} ? $data->{'meta'} : {};
        my $values   = exists $data->{'value'} ? $data->{'value'} : {};

        # Process metadata
        for my $meta (keys %$metadata) {

            my $attr = $metadata->{$meta};

            $attr->{'data_type'} = 'metadata';

            if ($attr->{'source'} =~ /.*\.+.*/) {
                $attr->{'source_type'} = 'oid';
                $attr->{'map'} = $self->_map_oid($meta, $attr, 'data');
            }
            else {
                $attr->{'source_type'} = 'scan';
            }
        }

        # Process values
        for my $value (keys %$values) {
            
            my $attr = $values->{$value};

            $attr->{'data_type'} = 'value';

            if ($attr->{'source'} =~ /.*\.+.*/) {
                $attr->{'source_type'} = 'oid';
                $attr->{'map'} = $self->_map_oid($value, $attr, 'data');
            }
            else {
                $attr->{'source_type'} = 'scan';
            }
        }

        # Combine metadata and value elements for a single data entry for the composite
        %{$composite{'data'}} = (%$metadata, %$values);
         
        # Begin processing the given parameters for "conversions"
        # These are parsed using get * to preserve ordering
        my $conversions = $config->get('/composite/conversions/*');

        # The ordering of conversions must be preserved
        # For this reason, we init conversions as an array
        $composite{'conversions'} = [];

        # Assign the conversion type and parameters based on XML element attributes
        for my $c (@$conversions) {

            # Set the array of data targets the conversion is applied to
            my $targets = [keys %{$c->{'data'}}];

            # Create a hash for the conversion with the data targets set
            my $conversion = { data => $targets };

            # Add all of the attributes from the config to the conversion
            for my $attr (keys %$c) {
                $conversion->{$attr} = $c->{$attr} if ($attr ne 'data');
            }

            # Choose the conversion type based upon required attributes
            $conversion->{'type'} = 'function' if (exists $c->{'definition'});
            $conversion->{'type'} = 'replace' if (exists $c->{'this'});
            $conversion->{'type'} = 'match' if (exists $c->{'pattern'});
            $conversion->{'type'} = 'drop' if (!$conversion->{'type'});

            push($composite{'conversions'}, $conversion);
        }

        # We want to get the config hash here so it isn't reparsed by the worker
        $composites{$composite_name} = \%composite;
    }

    $self->_set_composites(\%composites);

    $self->logger->info("Composite definitions read successfully: " . scalar(keys %composites));
}


=head2 _map_oid
    Creates a map hash for an OID having:
    - split_oid (The OID split into it's elements)
    - base_oid  (The OID made from the max elems we can poll w/o having vars in it)
    - first_var (The index position of the first var elem)
    - last_var  (The index position of the last var elem)
    - vars      (A hash of OID elem vars set to their index in the OID)
=cut
sub _map_oid {

    my $self = shift;
    my $name = shift;
    my $elem = shift;
    my $type = shift;

    # Init the hash map to return for the OID
    my %oid_map;

    # Init the base OID to be requested from Redis by Data
    # The attribute containing it differs by element type
    if ($type eq 'scan') {
        $oid_map{'base_oid'} = $elem->{'oid'};
    }
    elsif ($type eq 'data') {
        $oid_map{'base_oid'} = $elem->{'source'};
    }
    else {
        # OIDs are always contained within an "oid" or "source" attribute
        $self->logger->error("ERROR: Invalid value for \"type\" given to _map_oid");
    }

    # Split the oid into its nodes
    my @split_oid = split(/\./, $oid_map{'base_oid'});

    # Save the split_oid in the map for Workers to use later
    $oid_map{'split_oid'} = \@split_oid;

    # We track the first and last index of the OID nodes before the first OID variable
    # These indexes are then used to join portions of the OID not containing variable OID nodes
    my $first_index;
    my $last_index;

    # Loop over the OID nodes
    for (my $i = 0; $i <= $#split_oid; $i++) {

        # Get the specific node from the OID
        my $oid_node = $split_oid[$i];

        # Change * to the name of the element for backward compatibility
        if ($oid_node eq '*') {
            $oid_node      = $name;
            $split_oid[$i] = $name;
        }

        # Check if the OID node is an OID node variable, including *
        # The regex will match for standard varriable naming conventions
        if ($oid_node =~ /^((?![\s\d])\$?[a-z]+[\da-z_-]*)*$/i) {

            # Add the name of the variable OID node and its index to the map
            $oid_map{'vars'}{$oid_node}{'index'} = $i;

            # Initialize the first and last normal OID node indexes
            if (!defined $first_index) {
                $first_index = $i;
                $last_index  = $i;
            }

            # Update the last OID node index
            $last_index = $i if ($last_index < $i);
        }
    }

    $oid_map{'first_var'} = $first_index;
    $oid_map{'last_var'}  = $last_index;

    # Update the base OID when variable OID nodes were found
    # The first/last index will be undefined if not found
    if ($first_index) {
        # The .* is appended for simp-data's request to Redis
        # Without it, Data will match any OID suffix starting with that num
        # i.e. (1.2.3.1 would get 1.2.3.10, 1.2.3.17, 1.2.3.108, etc.)
        $oid_map{'base_oid'} = (join '.', @split_oid[0 .. ($first_index - 1)]) . '.*';
    }

    return \%oid_map;
}


=head2 start
    Starts the simp_comp process and creates all of its workers
=cut
sub start
{
    my ($self) = @_;

    $self->logger->info('Starting.');
    $self->logger->debug('Setting up signal handlers.');

    # setup signal handlers
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

        # Set the PID file to use from config or default
        my $pid_file;
        if (!exists $self->config->{'pid-file'}) {
            $pid_file = "/var/run/simp_comp.pid";
        }
        else {
            $pid_file = $self->config->{'pid-file'};
        }
        $self->logger->debug("PID FILE: " . $pid_file);

        my $daemon = Proc::Daemon->new(pid_file => $pid_file);

        my $pid = $daemon->Init();

        # in child/daemon process
        if (!$pid) {

            $self->logger->debug('Created daemon process.');

            # change process name
            $0 = "simp_comp [master]";

            # figure out what user/group (if any) to change to
            my $user_name  = $self->run_user;
            my $group_name = $self->run_group;

            if (defined($group_name)) {

                my $gid = getgrnam($group_name);
                $self->_log_and_exit("Unable to get GID for group '$group_name'") if !defined($gid);
                $! = 0;
                setgid($gid);
                $self->_log_and_exit("Unable to set GID to $gid ($group_name)") if $! != 0;
            }

            if (defined($user_name)) {

                my $uid = getpwnam($user_name);
                $self->_log_and_exit("Unable to get UID for user '$user_name'") if !defined($uid);
                $! = 0;
                setuid($uid);
                $self->_log_and_exit("Unable to set UID to $uid ($user_name)") if $! != 0;
            }

            $self->_create_workers();
        }

        # Run in the foreground
    }
    else {

        $self->logger->debug('Running in foreground.');

        # when in fg just act as a working directly with no sub-processes
        # so we can nytprof
        my $worker = GRNOC::Simp::Comp::Worker->new(
            config     => $self->config,
            rmq_config => $self->rmq_config,
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

    exit(1);
}


=head2 stop
    Kills all simp_comp processes
=cut
sub stop {

    my ($self) = @_;

    $self->logger->info('Stopping.');

    my @pids = @{$self->children};

    $self->logger->debug('Stopping child worker processes ' . join(' ', @pids) . '.');

    return kill('TERM', @pids);
}


=head2 _create_workers
    Creates all simp_comp workers
=cut
sub _create_workers {

    my ($self) = @_;

    my $workers = '1';
    if (exists $self->config->{'workers'}) {
        $workers = $self->config->{'workers'};
    }

    $self->logger->info("Creating $workers child worker processes.");

    my $forker = Parallel::ForkManager->new($workers);

    # keep track of children pids
    $forker->run_on_start(
        sub {
            my ($pid) = @_;
            $self->logger->debug("Child worker process $pid created.");
            push(@{$self->children}, $pid);
        }
    );

    # create high res workers
    for (my $worker_id = 0; $worker_id < $workers; $worker_id++) {
        $forker->start() and next;

        # create worker in this process
        my $worker = GRNOC::Simp::Comp::Worker->new(
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

    $self->logger->debug('Waiting for all child worker processes to exit.');

    # wait for all children to return
    $forker->wait_all_children();

    $self->_set_children([]);

    $self->logger->debug('All child workers have exited.');
}

1;
