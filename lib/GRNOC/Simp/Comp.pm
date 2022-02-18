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

our $VERSION = '1.8.3';

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
        $self->logger->error("ERROR: Failed to validate $file!");
        $self->logger->error($conf->{error}->{error});
        $self->logger->error($conf->{error}->{backtrace});
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
    @files = grep { $_ =~ /^[^.#][^#]*\.xml$/ } readdir($dir);
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
        my %composite = (name => $composite_name);
        
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

        # Process scans, preserving ordering
        $composite{'scans'} = [];
        if (exists $variables->{'scan'} && ref $variables->{'scan'} eq 'ARRAY') {

            for (my $i = 0; $i <= $#{$variables->{'scan'}}; $i++) {

                my $scan = $variables->{'scan'}[$i];

                # We must get matches by indexing the scan using XPath.
                # XPath indexing starts at 1, not 0
                # This allows us to preserve ordering of both the scan and matches it has
                my $match_conf = $config->get(sprintf("/composite/variables/scan[%s]/match", $i+1));
                    
                my %match_index;
                my %match_names = map {$_->{name} => 1} @$match_conf;
                my @match_order = map {$_->{name}}      @$match_conf;
                my $match_count = scalar(@match_order);

                # TODO: Not all of these metadata pieces are necessary.
                #       They've been left in the case they're useful later.
                #       If not, we might remove some of the extra metadata.
                my %matches = (
                    index => \%match_index,
                    names => \%match_names,
                    order => \@match_order,
                    count =>  $match_count,
                );

                # For validation, get a count of matches and an array of the corresponding asterisks
                my @asterisks   = $scan->{oid} =~ /(\*|\*\*)+/g;

                # Verify:
                #   At least one <match> element exists
                #   At least one variable OID node exists (* and/or **)
                #   The number of match elements is the same as variable OID nodes
                my $match_err;

                if ($matches{count} == 0) {
                    $match_err = '[%s] ERROR: Scan configuration has no defined match elements!';
                }
                elsif (scalar(@asterisks) == 0) {
                    $match_err = '[%s] ERROR: Scan configuration missing * or ** corresponding to matches!';
                }
                elsif (scalar(@asterisks) != $matches{count}) {
                    $match_err = '[%s] ERROR: Wrong number of match elements defined for scan in configuration!';
                }

                # Log our validation error and exit if one was found
                if ($match_err) {
                    $self->_log_and_exit(sprintf($match_err, $composite_name))
                }

                # Create a hash of the scan parameters for the Workers to use.
                my $scan_params = {
                    'oid'     => $scan->{oid},
                    'value'   => $scan->{value},
                    'matches' => \%matches,
                    'index'   => $i # Index used to preserve ordering for async responses 
                };

                # Create helpful metadata about the scan's OID, adding it to the scan's parameters
                $self->_map_oid($scan_params, 'scan');

                push(@{$composite{'scans'}}, $scan_params);
            }
        }

        # Begin processing the given parameters for "data"
        my $data = $parameters->{'data'}[0];
        my $metadata = exists $data->{'meta'} ? $data->{'meta'} : {};
        my $values   = exists $data->{'value'} ? $data->{'value'} : {};

        # Process metadata
        for my $meta (keys %{$data->{'meta'}}) {

            my $attr = $data->{'meta'}{$meta};

            $attr->{'data_type'} = 'metadata';
            $attr->{'name'}      = $meta;

            if ($attr->{'source'} =~ /.*\.+.*/) {
                $attr->{'source_type'} = 'oid';
                $self->_map_oid($attr, 'data');
            }
            elsif (exists($composite{constants}{$attr->{'source'}})) {
                $attr->{'source_type'} = 'constant';
            }
            else {
                $attr->{'source_type'} = 'scan';
            }
        }

        # Process values
        for my $value (keys %{$data->{'value'}}) {
            
            my $attr = $data->{'value'}{$value};

            $attr->{'data_type'} = 'value';
            $attr->{'name'}      = $value;

            if ($attr->{'source'} =~ /.*\.+.*/) {
                $attr->{'source_type'} = 'oid';
                $self->_map_oid($attr, 'data');
            }
            elsif (exists($composite{constants}{$attr->{'source'}})) {
                $attr->{'source_type'} = 'constant';
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

	    # If the conversation if a replacement AND we have a subroutine
	    # as the 'with' parameter, make it eval'd in a safe way so that
	    # it actually gets evaluated properly later.
	    if ($conversion->{'type'} eq 'replace' &&
		$conversion->{'with'} =~ /^\s*sub\s*{/) {
		my $new_with = Safe->new()->reval($conversion->{'with'});
		if (! $new_with || ! ref $new_with eq 'CODE') {
		    $self->_log_and_exit("ERROR: Cannot parse conversion \"with\" attribute as subroutine code ->" . $@);
		}
		$conversion->{'with'} = $new_with;
	    }

            push($composite{'conversions'}, $conversion);
        }

        # We want to get the config hash here so it isn't reparsed by the worker
        $composites{$composite_name} = \%composite;
    }

    $self->_set_composites(\%composites);

    $self->logger->info("Composite definitions read successfully: " . scalar(keys %composites));
}


=head2 _map_oid
    Parses an OID and maps out variables specific to the OID:
    - base_oid (The OID made from the max elems we can poll w/o having vars in it)
    - oid_vars (Array of OID variables in order of appearance)
    - regex    (Regex that matches an OID capturing OID variables in matching order of oid_vars)
=cut
sub _map_oid {

    my $self = shift;
    my $elem = shift;
    my $type = shift;

    # Init the base OID to be requested from Redis by Data
    # The attribute containing it differs by element type
    if ($type eq 'scan') {
        $elem->{'base_oid'} = $elem->{'oid'};
    }
    elsif ($type eq 'data') {
        $elem->{'base_oid'} = $elem->{'source'};
    }
    else {
        # OIDs are always contained within an "oid" or "source" attribute
        $self->logger->error("ERROR: Invalid value for \"type\" given to _map_oid");
    }


    # If the OID has a leading dot, strip it for the sake
    # of parsing the OID but we'll add it back later to make
    # sure it lines up with poller
    my $leading_dot = 0;
    if ($elem->{'base_oid'} =~ /^\./){
	$leading_dot = 1;
	$elem->{'base_oid'} =~ s/^\.//;
    }

    # Split the oid into its nodes
    my @split_oid = split(/\./, $elem->{'base_oid'});

    # We track the first index of the OID nodes before the first OID variable
    # The index is then used to join portions of the OID not containing variable OID nodes
    my $first_index;

    # Create a regex pattern that will capture OID variables and any tailing OID nodes
    my @re_elems;

    # Create an array of the OID variables as they occur preserving ordering
    my @oid_vars;

    # Track the sequence we see asterisks in as they are mapped to match element names
    my $match_index = 0;

    # Loop over the OID nodes
    for (my $i = 0; $i <= $#split_oid; $i++) {

        # Get the specific node from the OID
        my $oid_node = $split_oid[$i];

        # Determine what we should add to our regex
        my $re_elem = '';

        # Check for asterisks which indicate an OID node match mapping
        if ($type eq 'scan' && ($oid_node eq '**' || $oid_node eq '*')) {

            # Get the match element that maps to the current asterisk
            my $match = $elem->{matches}{order}[$match_index];

            # Set the index of the node in the OID where the match occurs
            $elem->{matches}{index}{$i} = $match;

            # Increase the match index for the next occurrence of a match position
            $match_index++;

            # *  Captures the number in the OID exactly at position $i
            # ** Captures everything in the OID from position $i to the right
            $re_elem = ($oid_node eq '**') ? '([\d\.]+)' : '(\d+)';

            # Change the asterisk(s) to the match element's name
            $oid_node      = $match;
            $split_oid[$i] = $match;

            push(@oid_vars, $oid_node);

            $first_index = $i if (!defined $first_index);
        }
        # Check if the OID node is an OID node variable, including *
        # The regex will match for standard variable naming conventions
        elsif ($oid_node =~ /^((?![\s\d])\$?[a-z]+[\da-z_-]*)*$/i) {

            # Add the name of the variable OID node and its index to the map
            push(@oid_vars, $oid_node);

            # Initialize the first variable OID node index
            $first_index = $i if (!defined $first_index);

	    # Since it's a variable we need to potentially
	    # match multiple sections for a ** match
            $re_elem = '([\d\.]+)';
        }
        else {
            # Add the constant to our regex
            $re_elem = $oid_node;
        }

        push(@re_elems, $re_elem);
    }

    # Set the regex pattern for the OID
    my $regex = '^' . ($leading_dot ? "." : "") . join('\.', @re_elems);
    $elem->{'regex'} = qr/$regex/;

    # Set the ordered oid_vars array
    $elem->{'oid_vars'} = \@oid_vars;

    # Update the base OID when variable OID nodes were found
    # The first/last index will be undefined if not found
    if ($first_index) {

        # The .* is appended for simp-data's request to Redis
        $elem->{'base_oid'} = ($leading_dot ? "." : "") . (join '.', @split_oid[0 .. ($first_index - 1)]) . '.*';
    }
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
        my $pid_file = exists($self->config->{'pid-file'}) ? $self->config->{'pid-file'} : "/var/run/simp_comp.pid";

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
    }
    # Foreground
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

    my $workers = exists $self->config->{'workers'} ? $self->config->{'workers'} : '1';

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
            rmq_config => $self->rmq_config,
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
