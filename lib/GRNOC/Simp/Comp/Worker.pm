package GRNOC::Simp::Comp::Worker;

use strict;
use warnings;

### REQUIRED IMPORTS ###
use AnyEvent;
use Carp;
use Data::Dumper;
use Data::Munge qw();
use List::MoreUtils qw(any);
use Moo;
use Time::HiRes qw(gettimeofday tv_interval);
use Try::Tiny;

use GRNOC::Config;
use GRNOC::Log;
use GRNOC::RabbitMQ::Client;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Method;
use GRNOC::WebService::Regex;

### REQUIRED ATTRIBUTES ###

=head1 public attributes

=over 12

=item config
=item rmq_config
=item logger
=item composites
=item worker_id

=back

=cut

has config => (
    is       => 'ro',
    required => 1
);

has rmq_config => (
    is       => 'ro',
    required => 1
);

has logger => (
    is       => 'ro',
    required => 1
);

has composites => (
    is       => 'ro',
    required => 1
);

has worker_id => (
    is       => 'ro',
    required => 1
);

### INTERNAL ATTRIBUTES ###

=head2 private attributes

=over 12

=item is_running
=item dispatcher
=item rmq_client
=item do_shutdown
=item rmq_dispatcher

=back

=cut

has is_running => (
    is      => 'rwp',
    default => 0
);

has dispatcher => (
    is => 'rwp'
);

has rmq_client => (
    is => 'rwp'
);

has do_shutdown => (
    is      => 'rwp',
    default => 0
);

has rmq_dispatcher => (
    is      => 'rwp',
    default => sub { undef }
);

my %_FUNCTIONS;    # Used by _function_one_val
my %_RPN_FUNCS;    # Used by _rpn_calc

### PUBLIC METHODS ###

=head2 public_methods

=over 12

=item start

=back

=cut


=head2 start
    Starts the Comp Worker process
=cut
sub start {
    my ($self) = @_;

    $self->_set_do_shutdown(0);

    while (1) {

        # we use try catch to, react to issues such as com failure
        # when any error condition is found,
        # the reactor stops and we then reinitialize
        $self->logger->debug($self->worker_id . " restarting.");

        $self->_start();

        exit(0) if $self->do_shutdown;

        sleep 2;
    }
}


=head2 _check_rabbitmq()
    Checks the whether the RabbitMQ client and dispatcher are connected.
    If either is not connected it will return false.
=cut
sub _check_rabbitmq {
    my $self      = shift;

    my $client_conn = $self->rmq_client && $self->rmq_client->connected ? 1 : 0;
    my $dispat_conn = $self->rmq_dispatcher && $self->rmq_dispatcher->connected ? 1 : 0;

    return $client_conn && $dispat_conn ? 1 : 0;
}


=head2 _connect_rmq_client()
    Creates a RabbitMQ Client and sets it for the worker
=cut
sub _connect_rmq_client {
    my $self = shift;

    $self->logger->debug("Connecting the RabbitMQ client");

    my $rmq_client = GRNOC::RabbitMQ::Client->new(
        host           => $self->rmq_config->{'ip'},
        port           => $self->rmq_config->{'port'},
        user           => $self->rmq_config->{'user'},
        pass           => $self->rmq_config->{'password'},
        exchange       => 'Simp',
        topic          => 'Simp.Data',
        timeout        => 15,
    );
    $self->_set_rmq_client($rmq_client);
}

=head2 _connect_rmq_dispatcher()
    Creates a RabbitMQ Dispatcher and sets it for the worker
=cut
sub _connect_rmq_dispatcher {
    my $self = shift;

    $self->logger->debug("Connecting the RabbitMQ dispatcher");

    my $rmq_dispatcher = GRNOC::RabbitMQ::Dispatcher->new(
        host           => $self->rmq_config->{'ip'},
        port           => $self->rmq_config->{'port'},
        user           => $self->rmq_config->{'user'},
        pass           => $self->rmq_config->{'password'},
        exchange       => 'Simp',
        topic          => 'Simp.Comp',
        queue_name     => 'Simp.Comp',
    );
    $self->_set_rmq_dispatcher($rmq_dispatcher);
}

=head2 _setup_rabbitmq
    This will set up the RabbitMQ rmq_client and dispatcher for the worker.
    It also create all of the methods for RMQ from the composites.
=cut
sub _setup_rabbitmq {
    my ($self) = @_;

    $self->logger->debug('Setting up RabbitMQ');

    $self->_connect_rmq_client();
    $self->_connect_rmq_dispatcher();

    my %predefined_param = map { $_ => 1 } ('node', 'period', 'exclude_regexp');

    # Register every composite as a RabbitMQ method calling _get() using its parameters
    for my $composite_name (keys %{$self->composites}) {

        my $composite = $self->composites->{$composite_name};

        my $method = GRNOC::RabbitMQ::Method->new(
            name        => "$composite_name",
            async       => 1,
            callback    => sub { $self->_get($composite, @_) },
            description => "Retrieve composite data of type \"$composite_name\""
        );

        $method->add_input_parameter(
            name        => 'node',
            description => 'Nodes to retrieve data for',
            required    => 1,
            multiple    => 1,
            pattern     => $GRNOC::WebService::Regex::TEXT
        );

        $method->add_input_parameter(
            name        => 'period',
            description => "Period of time to request for the data",
            required    => 0,
            multiple    => 0,
            pattern     => $GRNOC::WebService::Regex::ANY_NUMBER
        );

        # Process any input variables the composite has
        my $inputs = [];
        if (exists $composite->{'variables'}[0]{'input'}) {
            $inputs = $composite->{'variables'}[0]{'input'};
        }

        while ( my ($input, $attr) = each(%{$composite->{'inputs'}}) ) {

            unless (exists $predefined_param{$input}) {

                $method->add_input_parameter(
                    name        => $input,
                    description => "we will add description to the config file later",
                    required    => exists $attr->{'required'} ? $attr->{'required'} : 1,
                    multiple    => 1,
                    pattern     => $GRNOC::WebService::Regex::TEXT
                );
            }
        }

        $self->rmq_dispatcher->register_method($method);
    }

    $self->config->{'force_array'} = 0;

    # Create the RabbitMQ ping method
    my $ping = GRNOC::RabbitMQ::Method->new(
        name        => "ping",
        callback    => sub { $self->_ping() },
        description => "function to test latency"
    );
    $self->rmq_dispatcher->register_method($ping);

    # Begin the event loop that handles requests coming
    # in from the RabbitMQ queue
    if ($self->_check_rabbitmq()) {
        $self->logger->debug('Entering RabbitMQ event loop');
        $self->rmq_dispatcher->start_consuming();
    }
    else {
        $self->logger->error('ERROR: Simp.Comp worker could not connect to RabbitMQ, retrying...');
    }

    # If a handler calls "stop_consuming()" it removes
    # the dispatcher definition and returns
    $self->_set_rmq_dispatcher(undef);
}


=head2 _start
    This is the internal method used to start up a Worker
=cut
sub _start {
    my ($self) = @_;

    my $worker_id = $self->worker_id;

    # flag that we're running
    $self->_set_is_running(1);

    # change our process name
    $0 = "simp_comp ($worker_id) [worker]";

    # setup signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info("Received SIG TERM.");
        $self->_stop();
    };

    $SIG{'HUP'} = sub {
        $self->logger->info("Received SIG HUP.");
    };

    $self->_setup_rabbitmq();
}


### PRIVATE METHODS ###

=head2 _stop
    Stops the Comp Worker
=cut
sub _stop {
    my $self = shift;
    $self->_set_do_shutdown(1);

    my $dispatcher = $self->rmq_dispatcher;
    $dispatcher->stop_consuming() if defined($dispatcher);
}


=head2 _ping
    Used to check connectivity to RabbitMQ
=cut
sub _ping {
    my $self = shift;
    return gettimeofday();
}


=head2 _get
    This is the primary method invoked when TSDS requests data
    It will run through retrieving data from Redis, processing, and sending the result
=cut
sub _get {
    my $start     = [gettimeofday];
    my $self      = shift;
    my $composite = shift;
    my $rpc_ref   = shift;
    my $args      = shift;

    # Initialize the environment hash for _get to pass from method to method
    my $env = $self->_init_environment($args, $composite);

    # RabbitMQ callback that handles our final results
    my $success_callback = $rpc_ref->{'success_callback'};

    # Initialize the AnyEvent conditional variables for the async processing pipeline
    my @cv = map { AnyEvent->condvar; } (0 .. 4);

    # Below is the async processing pipeline
    # We step through each process using AnyEvent and our conditional variables

    # Gather data for scan elements
    $cv[0]->begin(sub { $self->_gather_data('scans', $env, $composite, $cv[1]); });
    
    # Gather data for data elements
    $cv[1]->begin(sub { $self->_gather_data('data', $env, $composite, $cv[2]); });
    
    # Digest the gathered data into an array of data objects
    $cv[2]->begin(sub { $self->_digest_data($env, $composite, $cv[3]); });
    
    # Apply conversions to the array of data objects
    $cv[3]->begin(sub { $self->_convert_data($composite, $args, $env, $cv[4]); });
    
    # Send the final data back and reset variables
    $cv[4]->begin(
        sub {
            my $end = [gettimeofday];
            my $resp_time = tv_interval($start, $end);
            $self->logger->info("REQTIME COMP $resp_time");
            &$success_callback($env->{'final'});
            undef $env;
            undef $composite;
            undef $args;
            undef @cv;
            undef $success_callback;
        }
    );


    # Reset the async pipeline:
    $cv[0]->end;
}


=head2 _init_environment()
    Initializes the environment hash containing all data about a single _get request.
    The environment is cleared after _get finishes running.
=cut
sub _init_environment {
    my $self      = shift;
    my $args      = shift;
    my $composite = shift;

    $self->logger->debug("Initializing the environment hash");

    # Create the environment hash
    my %env = (
        hosts     => $args->{node}{value},
        interval  => $args->{period}{value} || 60,
        final     => {},
        var_map   => {},
        constants => {},
        data      => {},
        scans     => {},
        meta      => {},
    );

    # Map any input variables for the composite
    if ($composite->{'inputs'}) {
        while ( my ($input, $attr) = each(%{$composite->{'inputs'}}) ) {
            $env{'var_map'}{$attr->{'value'}} = $input;
        }
    }

    # Map any constants for the composite
    if ($composite->{'constants'}) {
        while ( my ($const, $value) = each(%{$composite->{'constants'}}) ) {
            $env{'constants'}{$const} = $value;
        }
    }

    # Create host-specific hash entries
    for my $host (@{$env{hosts}}) {
        $env{data}{$host}  = [];
        $env{scans}{$host} = {}
    }

    # Map scan poll_value and oid_suffix names
    while ( my ($name, $attr) = each(%{$composite->{scans}}) ) {
        $env{'var_map'}{$name} = $attr->{suffix};
    }

    # Map metadata names for TSDS
    while ( my ($name, $attr) = each(%{$composite->{data}}) ) {
        $env{'meta'}{$name} = "*$name" if ($attr->{data_type} eq 'metadata');
    }

    return \%env;
}


=head2 _gather_data()
    A method that will gather all OID data needed for the _get() request.
    The data is cached in the environment variable's "data" hash.
=cut
sub _gather_data {
    my $self      = shift;
    my $type      = shift;
    my $env       = shift;
    my $composite = shift;
    my $cv        = shift;

   $self->logger->debug("Gathering data for $type elements");

    # Process each composite element of the type
    while (my ($name, $attr) = each(%{$composite->{$type}})) {

        # Skip data elements where their source is not an OID
        next if ($type eq 'data' && $attr->{source_type} ne 'oid');

        # Request a cache of data for the OID
        $self->_request_data($env, $name, $attr, $cv);
    }

    # Signal that we have finished gathering all of the data
    $cv->end;
}


=head2 _request_data()
    A method that will request data from SIMP::Data/Redis.
    Handles one OID/composite element at a time.
    OID data is cached in $env->{data} by the callback.
=cut
sub _request_data {
    my $self = shift;
    my $env  = shift;
    my $name = shift;
    my $attr = shift;
    my $cv   = shift;

    # Get the base OID subtree to request from the element's map.
    # It is tempting to request just the OIDs you know you want,
    # instead of asking for the whole subtree, but posting a
    # bunch of requests for individual OIDs takes significantly
    # more time and CPU. That's why we request the subtree.
    my $oid = $attr->{map}{base_oid};

    $self->logger->debug("[$name] Requesting data");

    # Add to the AnyEvent condition variable to make the request async
    # We end it within the callback after data caching completes
    $cv->begin;

    # Flag for rate calculations
    my $rate = exists($attr->{type}) && defined($attr->{type}) && $attr->{type} eq 'rate';

    # Requests for a calculated rate value
    if ($rate) {
        $self->rmq_client->get_rate(
            node           => $env->{hosts},
            period         => $env->{interval},
            oidmatch       => [$oid],
            async_callback => sub {
                my $data = shift;
                $self->_cache_data($env, $name, $attr, $data->{results});
                $cv->end;
            }
        );
    }
    # Requests for ordinary OID values
    else {
        $self->rmq_client->get(
            node           => $env->{hosts},
            oidmatch       => [$oid],
            async_callback => sub {
                my $data = shift;
                $self->_cache_data($env, $name, $attr, $data->{results});
                $cv->end;
            }
        );
    }
}


=head2 _cache_data()
    A callback used to process OID data and cache it in the environment.
=cut
sub _cache_data {
    my $self = shift;
    my $env  = shift;
    my $name = shift;
    my $attr = shift;
    my $data = shift;

    $self->logger->debug("[$name] Started caching data");

    $self->debug_dump($attr);

    # Check whether Simp::Data retrieved anything from Redis
    if (!defined($data)) {
        $self->logger->error("[$name] Could not retrieve data from Redis using Simp::Data");
    }

    # Get the OID map
    my $map = $attr->{map};

    # Get some parameters from the map
    my $vars   = $map->{vars};
    my $start = $map->{first_var};

    # Determine if the element is a scan
    # Caching for scans is a little different
    my $scan = exists $attr->{vars} ? 1 : 0;

    for my $host (@{$env->{hosts}}) {

        # Skip the host when there's no data for it
        if (!exists($data->{$host}) || !defined($data->{$host})) {
            $self->logger->error("[$name] $host has no data to cache for this");
            next;
        }

        # Check all of the OID data returned for the host
        for my $oid (keys %{$data->{$host}}) {

            my $value = $data->{$host}{$oid}{value};
            my $time  = $data->{$host}{$oid}{time};

            # Only OIDs that have a value and time are kept
            next unless (defined $value && defined $time);

            # Here, we begin handling the OID itself.
            # Validate the OID returned against the requested OID.
            # The portions of the OID that aren't variables are compared.
            # If anything doesn't match, flag that its data is invalid for the request.
            # This handles constant OID elements between or after OID variables.
            # We start iterating over the OID elements at the first variable
            my $invalid = 0;
            my @split_oid = split(/\./, $oid);
            for (my $i = $start; $i <= $#split_oid; $i++) {

                # Get the OID element at the index in the data OID and the requested OID
                my $data_elem = $split_oid[$i];
                my $want_elem = @{$map->{split_oid}}[$i];

                # Here, we handle the use of OID variables from scans
                # The first check is for oid_suffix vars
                # The second check is for poll_value vars
                # Is the wanted OID element a scan suffix?
                if (exists($vars->{$want_elem})) {
                    $data->{$host}{$oid}{vars}{$want_elem} = $data_elem;
                    next;
                } 
                # Is the wanted OID element a scan value?
                elsif (exists($env->{var_map}{$want_elem})) {

                    # Assign the OID variable and value to the data then next element
                    $data->{$host}{$oid}{vars}{$env->{var_map}{$want_elem}} = $data_elem;
                    next;
                }

                
                # Check whether the OID constants match what was requested
                # If not, mark the data as invalid and end the loop
                if ($data_elem ne $want_elem) {
                    $invalid++;
                    last;
                }
            }

            # Here, we cache a data hash representing the OID.
            # Only cache data that is valid for the requested OID.
            unless ($invalid) {

                # Change the value key to the name of the value
                $data->{$host}{$oid}{$name} = delete $data->{$host}{$oid}{value};

                if ($scan) {
                    # Add scan data hashes to the scans hash
                    $env->{scans}{$host}{$name} = $data->{$host}{$oid};
                }
                else {
                    # Push to the data array for data elements
                    push(@{$env->{data}{$host}}, $data->{$host}{$oid});
                }
            }
        }
    }

    $self->logger->debug("[$name] Finished caching data");
}


=head2 _reference_scan()
    Gets scan data for a scan variable referenced by a data element's OID
    This handles the use of poll_value and oid_suffix vars in OIDs
=cut
sub _reference_scan {
    my $self = shift;
    my $host = shift; # The hostname
    my $var  = shift; # The OID variable name referencing a scan
    my $attr = shift;
    my $node = shift; # The OID node value (number)

    # Determine if the variable is an OID suffix or poll value
    my $is_value = $attr->{vars} ? 1 : 0;

    # Get the poll value name to reference the scan data
    my $name = $is_value ? $env->{var_map}{$var} : $var;

    my $poll_value = $attr->{vars}{$var} eq 'suffix'
    my $oid_suffix = 

    # Retrieve the scan data
    my $scan = $env->{scans}{$host}{$name};
}


=head2 _digest_data()
    Processes the cached data for a _get() request.
    Data is arranged in an array of unique data hashes.
    Each unique data hash has a unique combination of metadata and values.
    The composition of the data hash is determined by indexing OID nodes (numbers).
=cut
sub _digest_data {
    my ($self, $env, $composite, $cv) = @_;

    # Create the array to store data hashes in
    my @data;

    for my $host (@{$env->{hosts}}) {

        $self->logger->debug("[$host] Digesting data");

        my $host_data = $env->{data}{$host};

        while (my ($name, $attr) = each(%{$composite->{data}})) {

            my $map   = $attr->{map};
        }

    }

    $self->debug_dump($env) and die;

    # Signal that we have finished digesting the data
    $cv->end;
}


=head2 debug_dump
    This is a function to optimize debug logging involving calls to Data::Dumper
    It will only evaluate the calls to Dumper() and the passed variable if in debug mode
    Default behavior for logger->debug() will process data even when debug is not active
=cut
sub debug_dump {
     my $self  = shift;
     my $value = shift;

     $self->logger->debug({
        filter => \&Data::Dumper::Dumper, 
        value  => $value
     });
}


=head2 _trim_data
    Checks all branch and leaf elems of a val_tree, removing any not listed in map_tree
=cut
sub _trim_data {

    my $self     = shift;
    my $val_tree = shift;
    my $map_tree = shift;

    return if (ref($val_tree) ne ref({}));

    # Loop over the val keys at the reference root of val
    for my $key (keys %{$val_tree})
    {
        next if ($key eq 'value' || $key eq 'time');

        # Check if the key from val exists as a key in the map
        if (exists $map_tree->{$key})
        {
            # If the existing value points to another hash
            if (   ref($val_tree->{$key}) eq ref({})
                && ref($map_tree->{$key}) eq ref({}))
            {
                $self->_trim_data($val_tree->{$key}, $map_tree->{$key});
            }
        }
        else
        {
            $self->logger->debug(
                "$key not found in map_tree, removing it from val_tree!");
            delete $val_tree->{$key};
        }
    }

    return $val_tree;
}

# Transforms OID values and data into a hash tree, preserving OID element dependencies
sub _transform_oids {

    my $self = shift;
    my $oids = shift;
    my $data = shift;
    my $map  = shift;
    my $type = shift;

    # The final translation hash
    my %trans = ('vals' => {});

    my $is_scan = 0;
    if (defined $type && $type eq 'scan') {
        $trans{'blanks'} = {};
        $is_scan++;
    }
    else {
        $type = 'none';
    }

    my @legend;    # Store vars in order of parent->child

    for my $oid (@{$oids}) {

        # Get the OID's returned value from polling
        my $value = $data->{$oid};
        next if (!defined $value);

        if (!defined $map->{'first_var'}) {
            $trans{'vals'}{'static'} = $value;
            $trans{'legend'} = ['static'];

            if ($is_scan) {
                $trans{'blanks'}{'static'} = {};
            }

            next;
        }

        my @split_oid = split(/\./, $oid);

        # Check if the last var happens before the end of the data OID elements
        if (defined $map->{'last_var'} && $map->{'last_var'} < $#split_oid) {

            # Combine any tailing elements in the OID with the last var
            my $var_tail = join('.', splice(@split_oid, $map->{'last_var'}, $#split_oid));
            push(@split_oid, $var_tail);
        }

        # Make a reference point to the base of %vals
        my $ref = $trans{'vals'};
        my $blank_ref;

        # Do the same for blanks if it's for a scan
        if ($is_scan) {
            $blank_ref = $trans{'blanks'};
        }

        # Starting from the 1st var, loop over OID elements
        for (my $i = $map->{'first_var'}; $i <= $#split_oid; $i++) {

            # Get any matching var from the map's split OID
            my $var = $map->{'split_oid'}[$i];
            if (defined $var) {

                if (!exists $trans{'legend'}) {
                    push @legend, $var;
                }
            }
            else {
                next;
            }

            # Get the var's value in the polled OID
            my $oid_elem = $split_oid[$i];

            # If it's not a key at the reference point,
            # make it one and give it a val
            if (!exists $ref->{$oid_elem}) {

                # On the last key (leaf), assign the data
                if ($i == $#split_oid) {
                    $ref->{$oid_elem} = $value;
                    $ref->{$oid_elem}{'suffix'} = $oid_elem;
                }

                # Otherwise init a new hash in that key for another var
                # and set it in val results
                else {
                    $ref->{$oid_elem} = {};
                }

                # Always assign blank hashes to elems in the blanks hash tree
                if ($is_scan) {
                    $blank_ref->{$oid_elem} = {};
                }
            }

            # Switch the reference point to the elem's new hash
            $ref = $ref->{$oid_elem};

            if ($is_scan) {
                $blank_ref = $blank_ref->{$oid_elem};
            }
        }

        # Set the legend if it hasn't been
        if (!exists $trans{'legend'}) {
            $trans{'legend'} = \@legend;
        }
    }

    return \%trans;
}



# Adds all value leaves of a val_tree to the data_tree's hash leaves
sub _build_data {
    my $self      = shift;
    my $val_id    = shift;
    my $val_tree  = shift;
    my $data_tree = shift;

    # Check if our data tree reference is a leaf on the tree
    if (exists $data_tree->{'time'} || !scalar(%{$data_tree})) {

        # Ensure that we have a value to add to the leaf
        if (exists $val_tree->{'value'}) {

            # Set the value for the val
            $data_tree->{$val_id} = $val_tree->{'value'};

            # Set time once per leaf
            if (!exists $data_tree->{'time'} || !defined $data_tree->{'time'}) {

                # Set the time from the time defined for a value
                if (exists $val_tree->{'time'}) {
                    $data_tree->{'time'} = $val_tree->{'time'};
                }
                # Initialize the time field when neither val or data trees have a time
                # This is needed for the recursion to behave properly
                else {
                    $data_tree->{'time'} = undef;
                }
            }
        }
    }
    else {

        # Loop over the all the relevant keys of the data tree
        for my $key (keys %{$data_tree}) {

            # The data values haven't been reached yet
            if (!exists $val_tree->{'value'}) {

                # Check that the val_tree follows the path along the data tree
                if (exists $val_tree->{$key}) {

                    # Recurse with the new key in both hashes
                    $self->_build_data($val_id, $val_tree->{$key}, $data_tree->{$key});
                }
            }

            # The data values have been reached
            else {
                $self->_build_data($val_id, $val_tree, $data_tree->{$key});
            }
        }
    }
}


# Pushes all leaves of a data objects to an output array
sub _extract_data {

    my $self      = shift;
    my $data_tree = shift;
    my $output    = shift;

    for my $key (keys %{$data_tree}) {

        if (exists $data_tree->{$key}{'time'}) {
            push @{$output}, $data_tree->{$key};
        }
        else {
            $self->_extract_data($data_tree->{$key}, $output);
        }
    }
}


# Digests the val data and transforms it into an array of data objects after all callbacks complete
sub _derp_data {

    my $self      = shift;
    my $composite = shift;   # The instance XPath from the config
    my $params    = shift;   # Parameters to request
    my $results   = shift;   # Request-global $results hash
    my $cv        = shift;   # Assumes that it's been begin()'ed with a callback

    $self->logger->debug("Digesting vals");

    $self->debug_dump($results) and die;

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    for my $host (@$hosts) {

        # Handling for hosts when there's no value data returned
        if (!keys %{$results->{'scan_tree'}{$host}}) {
            $self->logger->error("No vals were returned for $host");
            $results->{'data'}{$host} = [];
            next;
        }

        # Reuse the scan tree for the host to build the data
        my $val_tree = $results->{'scan_tree'}{$host}{'vals'};

        # Get the vals polled for the host
        my $vals = $results->{'data'}{$host};

        # Add the data for all vals to the appropriate leaves of val_tree
        for my $val_id (keys %{$vals}) {
            $self->logger->debug("Building data for $val_id");
            $self->_build_data($val_id, $vals->{$val_id}, $val_tree);
        }

        $self->logger->debug("Finished building val data");

        # Construct the final, flattened data array from the completed val_tree
        my @val_data;
        $self->_extract_data($val_tree, \@val_data);
        $self->logger->debug("Extracted val data objects for $host");

        # Before applying conversions, we clean up the final array of data hashes
        #
        # First, we must ensure that each data hash contains a value for "time"
        # If no time has been set, we want to log an error
        #
        # Next, we ensure that all composite data elements have an entry in the hash.
        # OIDs for a data element that couldn't be collected will be missing their key.
        # Each data hash should always contain a key for every data element in the composite.
        # We initialize those keys to ensure this.
        #
        # Finally, we also take this opportunity to remove data hashes where a key
        # was marked required, but its value doesn't match the required regex.
        # This behavior can also be inverted to remove hashes where the value matches.

        # Check each data hash in the array in reverse order
        # Reverse order is necessary so that splicing doesn't alter the index of a hash
        for (my $i = $#val_data; $i >= 0; $i--) {

            # An individual data hash
            my $val_datum = $val_data[$i];

            # Ensure that a value for "time" has been set
            unless (exists $val_datum->{'time'} && defined $val_datum->{'time'}) {
                $self->logger->error("ERROR: Simp.Comp Worker produced data without a time value. This shouldn't be possible!");
            }

            # Initialize and apply required matching for every data element in the composite
            while ( my ($name, $attr) = each(%{$composite->{'data'}}) ) {

                # Initialize the data element's value as undef if it does not already exist
                $val_datum->{$name} = undef if (!exists($val_datum->{$name}));

                # Check if the data element's value is required to match a pattern
                if (exists($attr->{'require_match'})) {

                    # Does the value for the field match the given pattern?
                    # If so, we keep the data object (unless inverted)
                    my $match  = $val_datum->{$name} =~ $attr->{'require_match'};

                    # Indicates that we should drop the data hash on match instead of keeping it
                    my $invert = exists($attr->{'invert_match'}) ? $attr->{'invert_match'} : 0;

                    # Cases where we remove the data hash:
                    #   1. The pattern didn't match and we don't want to invert that
                    #   2. The pattern did match, but we're removing matches due to inversion
                    if ((!$match && !$invert) || ($match && $invert)) {
                        splice(@val_data, $i, 1);
                    }
                }
            }
        }

        # Set the results for val for the host to the data
        $results->{'data'}{$host} = \@val_data;
    }

    $self->logger->debug("Finished digesting vals");

    $cv->end;
}


# Applies conversion functions to values gathered by _get_data
sub _convert_data {

    my $self      = shift;
    my $composite = shift;    # The instance XPath from the config
    my $params    = shift;    # Parameters to request
    my $results   = shift;    # Global $results hash
    my $cv        = shift;    # AnyEvent condition var (assumes it's been begin()'ed)

    # Get the array of all conversions
    my $conversions = $composite->{'conversions'};

    $self->logger->debug("Applying conversions to the data");

    # Iterate over the data array for each host
    for my $host (keys %{$results->{'data'}}) {

        # Initialise the final data array for the host
        $results->{'final'}{$host} = [];

        # Skip and use empty results if host has no data
        if (!$results->{'data'}{$host} || (ref($results->{'data'}{$host}) ne ref([]))) {
            $self->logger->error("ERROR: No data array was generated for $host!");
            next;
        }

        # Set final values if no functions are being applied to the hosts's data
        unless ($conversions) {
            $results->{'final'}{$host} = $results->{'data'}{$host};
            next;
        }

        # Apply each function included in conversions
        for my $conversion (@$conversions) {

            # Get the target data for the conversion
            my $targets = $conversion->{'data'};

            # Apply the conversion to each data target
            for my $target (@$targets) {

                # Make a hash of all var names mentioned in the conversion
                # (avoids dupes)
                my %vars;

                # Initialize all conversion attributes specific to the target
                my $target_def     = $conversion->{'definition'};
                my $target_with    = $conversion->{'with'};
                my $target_this    = $conversion->{'this'};
                my $target_pattern = $conversion->{'pattern'};

                # Functions
                if ($conversion->{'type'} eq 'function') {

                    $target_def =~ s/\$\{\}/\$\{$target\}/g;

                    %vars = map { $_ => 1 } $target_def =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;

                    $self->logger->debug("Function has the following definition and vars:");
                    $self->debug_dump($target_def);
                    $self->debug_dump(\%vars);
                }

                # Replacements
                elsif ($conversion->{'type'} eq 'replace') {

                    $target_with =~ s/\$\{\}/\$\{$target\}/g;
                    $target_this =~ s/\$\{\}/\$\{$target\}/g;

                    # Combine strings so vars can be found easier in both
                    my $combo = $target_with . $target_this;

                    %vars = map { $_ => 1 } $combo =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;

                    $self->logger->debug("Replacement has the follwing vars:");
                    $self->debug_dump(\%vars);
                }

                # Matches
                elsif ($conversion->{'type'} eq 'match') {

                    $target_pattern =~ s/\$\{\}/\$\{$target\}/g;

                    %vars = map { $_ => 1 } $target_pattern =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;

                    unless (scalar(keys %vars)) {
                        $target_pattern = $conversion->{'pattern'};
                    }

                    $self->logger->debug("Match has the following pattern and vars:");
                    $self->debug_dump($target_pattern);
                    $self->debug_dump(\%vars);
                }

                # Apply the conversion for the target to each data object
                # for the host
                for my $data (@{$results->{'data'}{$host}}) {

                    # Initialize all temporary conversion attributes
                    # for the data object
                    my $temp_def     = $target_def;
                    my $temp_this    = $target_this;
                    my $temp_with    = $target_with;
                    my $temp_pattern = $target_pattern;

                    # Skip if the target data element doesn't exist
                    # in the object
                    next unless (exists $data->{$target} && defined $data->{$target});

                    my $conversion_err = 0;

                    # Replace data variables in the definition with their value
                    for my $var (keys %vars) {

                        # The value associated with the var for the data object
                        my $var_value;

                        # Check if there is data for the var, then assign it
                        if (exists($data->{$var}) && defined($data->{$var})) {
                            $var_value = $data->{$var};
                        }

                        # If not, see if the var is a user-defined constant,
                        # then assign it
                        elsif (exists($results->{'constants'}{$var})) {
                            $var_value = $results->{'constants'}{$var};
                        }

                        # If the var isn't anywhere, flag a conversion err
                        # and end the loop
                        else {
                            $conversion_err++;
                            last;
                        }

                        # Functions
                        if ($conversion->{'type'} eq 'function') {

                            # Replace the var indicators with their value
                            $temp_def =~ s/\$\{$var\}/$var_value/;
                            $self->logger->debug("Applying function: $temp_def");
                        }

                        # Replacements
                        elsif ($conversion->{'type'} eq 'replace') {

                            # Replace the var indicators with their value
                            $temp_with =~ s/\$\{$var\}/$var_value/g;
                            $temp_this =~ s/\$\{$var\}/$var_value/g;

                            $self->logger->debug("Replacing \"$temp_this\" with \"$temp_with\"");
                        }

                        # Matches
                        elsif ($conversion->{'type'} eq 'match') {

                            # Replace the var indicators with their value
                            $temp_pattern =~ s/\$\{$var\}/\Q$var_value\E/;
                            $self->debug_dump($temp_pattern);
                        }
                    }

                    # Don't send a value for the data if the conversion
                    # can't be completed as requested
                    if ($conversion_err) {
                        $data->{$target} = undef;
                        next;
                    }

                    # Store the converted value
                    my $new_value;

                    # Function application
                    if ($conversion->{'type'} eq 'function') {
                        # Use the FUNCTIONS object rpn definition
                        $new_value = $_FUNCTIONS{'rpn'}(
                            [$data->{$target}],
                            $temp_def, 
                            $conversion, 
                            $data, 
                            $results, 
                            $host
                        )->[0];
                    }

                    # Replacement application
                    elsif ($conversion->{'type'} eq 'replace') {

                        # Use the replace module to replace the value
                        $new_value = Data::Munge::replace(
                            $data->{$target}, 
                            $temp_this,
                            $temp_with
                        );
                    }

                    # Match application
                    elsif ($conversion->{'type'} eq 'match') {

                        # Change the output of the match where exclusion on match is desired
                        my $exclude = exists($conversion->{'exclude'}) ? $conversion->{'exclude'} : 0;
			my $drop_target = exists($conversion->{'drop'}) ? $conversion->{'drop'} : '';

                        # Set new value to pattern match or assign as undef
                        if ($data->{$target} =~ /$temp_pattern/) {
                            unless ($exclude == 1) {
                                $new_value = $1;
				if ( $drop_target ne '' ) {
				   delete $data->{$drop_target};
				   $self->logger->debug("Dropped $host, $drop_target: $data->{$drop_target}");
				}
			    }
                            else {
                                $new_value = undef;
                            }
                        }
                        else {
                            unless ($exclude == 1) {
                                $new_value = undef;
                            }
                            else {
                                $new_value = $data->{$target};
				if ( $drop_target ne '' ) {
				   delete $data->{$drop_target};
				   $self->logger->debug("Dropped $host, $drop_target: $data->{$drop_target}");
				}
                            }
                        }
                    }

                    # Drops
                    elsif ($conversion->{'type'} eq 'drop') {

                        $self->logger->debug("Dropping composite field $target");
                        delete $data->{$target};
                        next;
                    }

                    if (defined $new_value) {
                        $self->logger->debug("Set value for $target to $new_value");
                    }

                    # Assign the new value to the data target
                    $data->{$target} = $new_value;
                }
            }
        }

        for my $meta_name (keys %{$results->{'meta'}}) {
            for my $data (@{$results->{'data'}{$host}}) {
                if (exists $data->{$meta_name}) {
                    $data->{$results->{'meta'}->{$meta_name}} = delete $data->{$meta_name};
                }
            }
        }

        # Once any/all functions are applied in results->val for the host,
        # we set the final data to that hash
        $results->{'final'}{$host} = $results->{'data'}{$host};
    }

    $self->debug_dump($results->{'final'});
    $self->logger->debug("Finished applying conversions to the data");

    # trigger final callback -- send data to simp-tsds
    $cv->end;
}


# These functions are called from _function_one_val with several arguments:
#
# value, as computed to this point
# default operand attribute for function
# XML <fctn> element associated with this invocation of the function
# hash of values for this (host, OID suffix) pair
# full $results hash, as passed around in this module
# host name for current value
#
%_FUNCTIONS = (

    # For many of these operations, we take the view that
    # (undef op [anything]) should equal undef, hence line 2
    'sum' => sub {
        my ($vals, $operand) = @_;
        my $new_val = 0;
        for my $val (@$vals)
        {
            $new_val += $val;
        }
        return [$new_val];
    },
    'max' => sub {
        my ($vals, $operand) = @_;
        my $new_val;
        for my $val (@$vals)
        {
            if (!defined($new_val))
            {
                $new_val = $val;
            }
            else
            {
                if ($val > $new_val)
                {
                    $new_val = $val;
                }
            }
        }
        return [$new_val];
    },
    'min' => sub {
        my ($vals, $operand) = @_;
        my $new_val;
        for my $val (@$vals)
        {
            if (!defined($new_val))
            {
                $new_val = $val;
            }
            else
            {
                if ($val < $new_val)
                {
                    $new_val = $val;
                }
            }
        }
        return [$new_val];
    },
    '+' => sub {    # addition
        my ($vals, $operand) = @_;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            return [$val + $operand];
        }
    },
    '-' => sub {    # subtraction
        my ($vals, $operand) = @_;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            return [$val - $operand];
        }
    },
    '*' => sub {    # multiplication
        my ($vals, $operand) = @_;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            return [$val * $operand];
        }
    },
    '/' => sub {    # division
        my ($vals, $operand) = @_;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            return [$val / $operand];
        }
    },
    '%' => sub {    # modulus
        my ($vals, $operand) = @_;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            return [$val % $operand];
        }
    },
    'ln' => sub {    # base-e logarithm
        my $vals = shift;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            return [] if $val == 0;
            eval { $val = log($val); }; # if val==0, we want the result to be undef, so this works just fine
            return [$val];
        }
    },
    'log10' => sub {                    # base-10 logarithm
        my $vals = shift;
        for my $val (@$vals)
        {
            return [$val] if !defined($val);
            $val = eval { log($val); };    # see ln
            $val /= log(10) if defined($val);
            return [$val];
        }
    },
    'match' => sub {    # regular-expression match and extract first group
        my ($vals, $operand) = @_;
        for my $val (@$vals)
        {
            if ($val =~ /$operand/)
            {
                return [$1];
            }
            return [$val];
        }
    },
    'replace' => sub {    # regular-expression replace
        my ($vals, $operand, $replace_with) = @_;
        for my $val (@$vals)
        {
            $val = Data::Munge::replace($val, $operand, $replace_with);
            return [$val];
        }
    },
    'rpn' => sub {
        return [_rpn_calc(@_)];
    },
);


sub _rpn_calc
{
    my ($vals, $operand, $fctn_elem, $val_set, $results, $host) = @_;

    for my $val (@$vals)
    {
        # As a convenience, we initialize the stack with
        # a copy of $val on it already
        my @stack = ($val);

        # Split the RPN program's text into tokens (quoted strings,
        # or sequences of non-space chars beginning with a non-quote):
        my @prog;
        my $progtext = $operand;

        while (length($progtext) > 0)
        {
            $progtext =~
              /^(\s+|[^\'\"][^\s]*|\'([^\'\\]|\\.)*(\'|\\?$)|\"([^\"\\]|\\.)*(\"|\\?$))/;
            my $x = $1;
            push @prog, $x if $x !~ /^\s*$/;
            $progtext = substr $progtext, length($x);
        }

        my %func_lookup_errors;
        my @prog_copy = @prog;

        # Now, go through the program, one token at a time:
        for my $token (@prog)
        {
            # Handle some special cases of tokens:
            if ($token =~ /^[\'\"]/)
            {
                # quoted strings
                # Take off the start and end quotes, including
                # the handling of unterminated strings:
                if ($token =~ /^\"/)
                {
                    $token =~ s/^\"(([^\"\\]|\\.)*)[\"\\]?$/$1/;
                }
                else
                {
                    $token =~ s/^\'(([^\'\\]|\\.)*)[\'\\]?$/$1/;
                }

                $token =~ s/\\(.)/$1/g;    # unescape escapes
                push @stack, $token;
            }
            elsif ($token =~ /^[+-]?([0-9]+\.?|[0-9]*\.[0-9]+)$/)
            {
                # decimal numbers
                push @stack, ($token + 0);
            }
            elsif ($token =~ /^\$/)
            {
                # name of a value associated with the current (host, OID suffix)
                push @stack, $val_set->{substr $token, 1};
            }
            elsif ($token =~ /^\#/)
            {
                # host variable
                push @stack, $results->{'hostvar'}{$host}{substr $token, 1};
            }
            elsif ($token eq '@')
            {
                # push hostname
                push @stack, $host;
            }
            else
            {
                # treat as a function
                if (!defined($_RPN_FUNCS{$token}))
                {
                    GRNOC::Log::log_error("RPN function $token not defined!")
                      if !$func_lookup_errors{$token};
                    $func_lookup_errors{$token} = 1;

                    next;
                }

                $_RPN_FUNCS{$token}(\@stack);
            }

            # We copy, as in certain cases Dumper() can affect the
            # elements of values passed to it
            my @stack_copy = @stack;
        }

        # Return the top of the stack
        return pop @stack;
    }
}


# Turns truthy values to 1, falsy values to 0. Like K&R *intended*.
sub _bool_to_int {

    my $val = shift;
    return ($val) ? 1 : 0;
}


# Given a stack of arguments, mutate the stack
%_RPN_FUNCS = (

    # addend1 addend2 => sum
    '+' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? $a + $b : undef;
    },

    # minuend subtrahend => difference
    '-' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? $a - $b : undef;
    },

    # multiplicand1 multiplicand2 => product
    '*' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? $a * $b : undef;
    },

    # dividend divisor => quotient
    '/' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $x     = eval { $a / $b; };    # make divide by zero yield undef
        push @$stack, (defined($a) && defined($b)) ? $x : undef;
    },

    # dividend divisor => remainder
    '%' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $x     = eval { $a % $b; };    # make divide by zero yield undef
        push @$stack, (defined($a) && defined($b)) ? $x : undef;
    },

    # number => logarithm_base_e
    'ln' => sub {
        my $stack = shift;
        my $x     = pop @$stack;
        $x = eval { log($x); };           # make ln(0) yield undef
        push @$stack, $x;
    },

    # number => logarithm_base_10
    'log10' => sub {
        my $stack = shift;
        my $x     = pop @$stack;
        $x = eval { log($x); };           # make ln(0) yield undef
        $x /= log(10) if defined($x);
        push @$stack, $x;
    },

    # number => power
    'exp' => sub {
        my $stack = shift;
        my $x     = pop @$stack;
        $x = eval { exp($x); } if defined($x);
        push @$stack, $x;
    },

    # base exponent => power
    'pow' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $x     = eval { $a**$b; };
        push @$stack, (defined($a) && defined($b)) ? $x : undef;
    },

    # => undef
    '_' => sub {
        my $stack = shift;
        push @$stack, undef;
    },

    # a => (is a not undef?)
    'defined?' => sub {
        my $stack = shift;
        my $a     = pop @$stack;
        push @$stack, _bool_to_int(defined($a));
    },

    # a b => (is a numerically equal to b? (or both undef))
    '==' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res =
            (defined($a)  && defined($b))  ? ($a == $b)
          : (!defined($a) && !defined($b)) ? 1
          :                                  0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (is a numerically unequal to b?)
    '!=' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res =
            (defined($a)  && defined($b))  ? ($a != $b)
          : (!defined($a) && !defined($b)) ? 0
          :                                  1;
        push @$stack, _bool_to_int($res);
    },

    # a b => (is a numerically less than b?)
    '<' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a < $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (is a numerically less than or equal to b?)
    '<=' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a <= $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (is a numerically greater than b?)
    '>' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a > $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (is a numerically greater than or equal to b?)
    '>=' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a >= $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (Is string A equal to string B? (or both undef))
    'eq' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res =
            (defined($a)  && defined($b))  ? ($a eq $b)
          : (!defined($a) && !defined($b)) ? 1
          :                                  0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (Is string A unequal to string B?)
    'ne' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res =
            (defined($a)  && defined($b))  ? ($a ne $b)
          : (!defined($a) && !defined($b)) ? 0
          :                                  1;
        push @$stack, _bool_to_int($res);
    },

    # a b => (Is string A less than string B?)
    'lt' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a lt $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (Is string A less than or equal to string B?)
    'le' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a le $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (Is string A greater than string B?)
    'gt' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a gt $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (Is string A greater than or equal to string B?)
    'ge' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $res   = (defined($a) && defined($b)) ? ($a ge $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (a AND b)
    'and' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, _bool_to_int($a && $b);
    },

    # a b => (a OR b)
    'or' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, _bool_to_int($a || $b);
    },

    # a => (NOT a)
    'not' => sub {
        my $stack = shift;
        my $a     = pop @$stack;
        push @$stack, _bool_to_int(!$a);
    },

    # pred a b => (a if pred is true, b if pred is false)
    'ifelse' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $pred  = pop @$stack;
        push @$stack, (($pred) ? $a : $b);
    },

    # string pattern => match_group_1
    'match' => sub {
        my $stack   = shift;
        my $pattern = pop @$stack;
        my $string  = pop @$stack;
        if ($string =~ /$pattern/)
        {
            push @$stack, $1;
        }
        else
        {
            push @$stack, undef;
        }
    },

    # string match_pattern replacement_pattern => transformed_string
    'replace' => sub {
        my $stack       = shift;
        my $replacement = pop @$stack;
        my $pattern     = pop @$stack;
        my $string      = pop @$stack;

        if (!defined($string) || !defined($pattern) || !defined($replacement))
        {
            push @$stack, undef;
            return;
        }

        $string = Data::Munge::replace($string, $pattern, $replacement);
        push @$stack, $string;
    },

    # string1 string2 => string1string2
    'concat' => sub {
        my $stack   = shift;
        my $string2 = pop @$stack;
        my $string1 = pop @$stack;
        $string1 = '' if !defined($string1);
        $string2 = '' if !defined($string2);
        push @$stack, ($string1 . $string2);
    },

    # stealing some names from PostScript...
    #
    # a => --
    'pop' => sub {
        my $stack = shift;
        pop @$stack;
    },

    # a b => b a
    'exch' => sub {
        my $stack = shift;
        return if scalar(@$stack) < 2;
        my $b = pop @$stack;
        my $a = pop @$stack;
        push @$stack, $b, $a;
    },

    # a => a a
    'dup' => sub {
        my $stack = shift;
        return if scalar(@$stack) < 1;
        my $a = pop @$stack;
        push @$stack, $a, $a;
    },

    # obj_n ... obj_2 obj_1 n => obj_n ... obj_2 obj_1 obj_n
    'index' => sub {
        my $stack = shift;
        my $a     = pop @$stack;
        if (!defined($a) || ($a + 0) < 1)
        {
            push @$stack, undef;
            return;
        }
        push @$stack, $stack->[-($a + 0)];

        # This pushes undef if $a is greater than the stack size, which is OK
    },
);

1;
