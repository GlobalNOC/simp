package GRNOC::Simp::Comp::Worker;

use strict;
use warnings;

### REQUIRED IMPORTS ###
use Moo;
use AnyEvent;
use Data::Dumper;
use Data::Munge;
use Time::HiRes qw(gettimeofday tv_interval);
use Safe;

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

        # When any error condition is found, the worker reinitializes
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
        queue_name     => 'Simp.Comp'
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
            description => "Period of time to request for the data for rates",
            required    => 0,
            multiple    => 0,
            pattern     => $GRNOC::WebService::Regex::ANY_NUMBER
        );

        $method->add_input_parameter(
            name        => 'time',
            description => "Timestamp of the data to retrieve, defaults to now if not given",
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

    # Flag that the worker is running
    $self->_set_is_running(1);

    # Set our process name
    $0 = "simp_comp ($worker_id) [worker]";

    # Set signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info("Received SIG TERM.");
        $self->_stop();
    };
    $SIG{'HUP'} = sub {
        $self->logger->info("Received SIG HUP.");
    };

    # Setup RabbitMQ
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
    This is the primary method invoked when Simp::TSDS requests data through RabbitMQ
    It will run through retrieving data from Redis, processing it, and sending the result back
=cut
sub _get {
    my $start = [gettimeofday];
    my ($self, $composite, $rpc_ref, $args) = @_;

    # Initialize the request hash for _get to pass from method to method
    my $request = $self->_init_request($args, $composite);

    # RabbitMQ callback that sends our finished data
    my $success_callback = $rpc_ref->{'success_callback'};

    # Initialize the AnyEvent conditional variables for the async processing pipeline
    my @cv = map { AnyEvent->condvar; } (0 .. 1);

    # Gather the raw SNMP data for every host and OID in scans or data elements from Redis
    $cv[0]->begin(sub { $self->_gather_data($request, $cv[1]); });
    
    # Wait until the data has been fully gathered before performing any steps
    $cv[1]->begin(sub {

        # Digest the raw data into arrays of data objects per host
        $self->_digest_data($request);

        # Apply conversions to the digested host data
        $self->_convert_data($request);
        
        # Log the time to process the request 
        $self->logger->info("[$composite->{name}] REQTIME COMP " . tv_interval($start, [gettimeofday]));

        # Send results back to RabbitMQ for TSDS
        &$success_callback($request->{data});

        # Clear vars defined for the request
        undef $request;
        undef $composite;
        undef $args;
        undef @cv;
        undef $success_callback;
    });

    # Reset the async pipeline:
    $cv[0]->end;
}


=head2 _init_request()
    Initializes the request hash containing all data for a single _get request.
    The hash is cleared after _get finishes sending back the requested data.
=cut
sub _init_request {
    my ($self, $args, $composite) = @_;

    $self->logger->debug('['.$composite->{name}."] Initializing the request hash");

    # Create the request hash
    my %request = (
        composite => $composite,
        hosts     => $args->{node}{value},
        interval  => $args->{period}{value} || 60,
	time      => $args->{time}{value} || time(),
        constants => {},
        raw       => {scan => {}, data => {}},
        data      => {},
        meta      => {'node' => '*node'},
    );

    # Map any constants for the composite
    if ($composite->{'constants'}) {
        while ( my ($const, $value) = each(%{$composite->{'constants'}}) ) {
            $request{'constants'}{$const} = $value;
        }
    }

    # Create host-specific hash entries
    for my $host (@{$request{hosts}}) {
        $request{data}{$host} = [];
        $request{raw}{scan} = {};
        $request{raw}{data} = {};
    }

    # Map metadata names for TSDS
    for my $meta (keys %{$composite->{data}}) {
        $request{'meta'}{$meta} = "*$meta" if ($composite->{data}{$meta}{data_type} eq 'metadata');
    }

    return \%request;
}


=head2 _gather_data()
    A method that will gather all OID data needed for the _get() request.
=cut
sub _gather_data {
    my ($self, $request, $cv) = @_;

    my $composite = $request->{composite};

    $self->logger->debug('['.$composite->{name}."] Gathering data from Redis...");

    # Gather data for scans
    for my $scan (@{$composite->{'scans'}}) {   
        $self->_request_data($request, $scan->{value}, $scan, $cv);
    }

    # Gather data for data elements
    while (my ($name, $attr) = each(%{$composite->{'data'}})) {

        # Skip data elements where their source is a scan
        next if ($attr->{source_type} eq 'scan');

        # Skip data elements where their source is a constant
        next if ($attr->{source_type} eq 'constant');

        $self->_request_data($request, $name, $attr, $cv);
    }

    # Signal that we have finished gathering all of the data
    $cv->end;
}


=head2 _request_data()
    A method that will request data from SIMP::Data/Redis.
    Handles one OID/composite element at a time.
    OID data is cached in $request->{raw} by the callback.
=cut
sub _request_data {
    my ($self, $request, $name, $attr, $cv) = @_;

    # Get the base OID subtree to request.
    # It is tempting to request just the OIDs you know you want,
    # instead of asking for the whole subtree, but posting a
    # bunch of requests for individual OIDs takes significantly
    # more time and CPU. That's why we request the subtree.
    my $oid = $attr->{base_oid};

    # Add to the AnyEvent condition variable to make the request async
    # We end it within the callback after data caching completes
    $cv->begin;

    # Flag for rate calculations
    my $rate = exists($attr->{type}) && defined($attr->{type}) && $attr->{type} eq 'rate';

    # Requests for a calculated rate value
    if ($rate) {
        $self->rmq_client->get_rate(
            node           => $request->{hosts},
            period         => $request->{interval},
	    time           => $request->{time},
            oidmatch       => [$oid],
            async_callback => sub {
                my $data = shift;
                $self->_cache_data($request, $name, $attr, $data->{results});
                $cv->end;
            }
        );
    }
    # Requests for ordinary OID values
    else {
        $self->rmq_client->get(
            node           => $request->{hosts},
            oidmatch       => [$oid],
	    time           => $request->{time},
            async_callback => sub {
                my $data = shift;
                $self->_cache_data($request, $name, $attr, $data->{results});
                $cv->end;
            }
        );
    }
}


=head2 _cache_data()
    A callback used to cache raw OID data in the environment hash.
    Scans are cached in an array at $request->{raw}{scan}{$host}.
    Data are cached in a hash at $request->{raw}{data}{$host}.
    OIDs returned from Redis are checked to see if they match what was requested.
=cut
sub _cache_data {
    my ($self, $request, $name, $attr, $data) = @_;

    my $composite_name = $request->{composite}{name};

    # Check whether Simp::Data retrieved anything from Redis
    if (!defined($data)) {
        $self->logger->error("[$composite_name] Simp::Data could not retrieve \"$name\" from Redis");
        return;
    }

    # Determine the type of the element, scan or data element
    # Data elements have a 'source' attribute
    my $type = exists $attr->{source} ? 'data' : 'scan';

    # Cache data for host+port combinations returned from simp-data
    for my $host (@{$request->{hosts}}) {
        for my $port (keys(%$data)) {

            # Skip the host when there's no data for it
            if (!exists($data->{$port}{$host}) || !defined($data->{$port}{$host})) {
                $self->logger->error("[$composite_name] $host has no data to cache for \"$name\"");
                next;
            }

            # Raw scan data for a host needs to be wrapped in an array per individual scan
            my @scan_arr;

            for my $oid (keys %{$data->{$port}{$host}}) {

                my $value = $data->{$port}{$host}{$oid}{value};
                my $time  = $data->{$port}{$host}{$oid}{time};

                # Only OIDs that have a value and time are kept
                next unless (defined $value && defined $time);
            
                # Only OIDs with matching constant values and required OID vars present are kept
                next unless ($oid =~ $attr->{regex});

                # Scans are cached in a flat array
                if ($type eq 'scan') {

                    # Add the oid to the data to flatten for scans
                    $data->{$port}{$host}{$oid}{oid} = $oid;
                
                    # Push the scan data into the scan's array
                    push(@scan_arr, $data->{$port}{$host}{$oid});
                }
                # Data is cached in a flat hash
                else {
                    $request->{raw}{$type}{$port}{$host}{$oid} = $data->{$port}{$host}{$oid};
                }
            }

            # Push the array of raw data for the scan
            if ($type eq 'scan') {

                # Initialize the port hash for hosts
                unless (exists($request->{raw}{$type}{$port})) {
                    $request->{raw}{$type}{$port} = {};
                }
                # Initialize the array of scans for the host and port
                unless (exists $request->{raw}{$type}{$port}{$host}) {
                    $request->{raw}{$type}{$port}{$host} = [];
                }

                # Get the scan's index to preserve scan ordering
                # Async responses from multiple simp-data workers can cause unordered scan data caching
                my $scan_index = $attr->{index};

                # Assign the scan data to the appropriate index
                $request->{raw}{$type}{$port}{$host}[$scan_index] = \@scan_arr;
            }
        }
    }
}


=head2 _digest_data()
    Digests the raw data into arrays of data hashes per-host.
    This function wraps the data parser in a host loop.
=cut
sub _digest_data {
    my ($self, $request) = @_;

    $self->logger->debug("[".$request->{composite}{name}."] Digesting raw data");

    # Determine the unique ports used as the superset of ports returned for scans and data
    my %ports;
    @ports{keys(%{$request->{raw}{data}})} = ();
    @ports{keys(%{$request->{raw}{scan}})} = ();

    # Parse the data for each host and port
    for my $host (@{$request->{hosts}}) {
        for my $port (keys(%ports)) {

            # Skip the host if it has no data from sessions using the port
            next unless (
                exists($request->{raw}{data}{$port}{$host}) ||
                exists($request->{raw}{scan}{$port}{$host})
            );

            # Accumulate parsed data objects for the host+port to an array
            my @host_data;
            $self->_parse_data($request, $port, $host, \@host_data);

            # Add the accumulation to the data for the host
            push(@{$request->{data}{$host}}, @host_data);
        }
    }
}

=head2 _parse_data()
    Parses the raw data and pushes complete data objects for a host into an array recursively.
    Scans are recursed through in order, then a data object is built using those scan parameters.
    We pass an environment hash to each recursion containing KVPs for scan poll_values and oid_suffixes.
    When scans depth is exhausted, we build the data for the combination by calling _build_data().
=cut
sub _parse_data {

    my ($self, $request, $port, $host, $acc, $i, $env) = @_;

    # Initialize $i and $env for the first iterations
    $i   = 0 if (!defined($i));
    $env = {PORT => $port, NODE => $host} if ($i == 0);

    # Get the config for the scan
    my $scans = $request->{composite}{scans};
    my $scan  = $scans->[$i];

    # Once recursion hits a leaf, build data for the current environment
    if ($i > $#$scans) {
        my $output = $self->_build_data($request, $port, $host, $env);
        push(@$acc, $output) if ($output);
        return;
    }

    # The data for the scan
    my $data = $request->{raw}{scan}{$port}{$host}[$i];

    # The value name and match configs for the scan
    my $value_name  = $scan->{value};
    my $matches     = $scan->{matches};

    # Iterate over each data hash in the scan's array
    for (my $j = 0; $j <= $#$data; $j++) {

        my $datum = $data->[$j];
        my $oid   = $datum->{oid};

        # Create a copy of $env each time an OID changes
        my %env_copy = %$env;

        # Set the value name in $env to the value assigned to the OID
        $env_copy{$value_name} = $datum->{value};

        # Check if the OID matches the scan's regex and store any matching variables
        my @oid_vars = ($oid =~ $scan->{regex});

        # Skip non-matching OIDs
        next unless (@oid_vars);

        # Set time for $env from scan data in case we're only polling for scans
        if (!exists($env_copy{TIME}) || !defined($env_copy{TIME})) {
            $env_copy{TIME} = $datum->{time};
        }

        # Used for checking the validity of $env for the OID throughout parsing
        my $valid = 1;

        # Get EVERY variable node within the current OID
        for (my $var_index = 0; $var_index <= $#oid_vars; $var_index++) {

            my $var_name = $scan->{oid_vars}[$var_index];

                # Get the corresponding node value from the OID
                my $oid_node = $oid_vars[$var_index];

                # Confirm the node is right for $env
                if (exists($env_copy{$var_name}) && $env_copy{$var_name} != $oid_node) {
                    $valid = 0;
                    last;
                }
                
                # Set the OID node for the var in $env
                $env_copy{$var_name} = $oid_node;
        }

        # Skip any OID that was marked invalid while gathering matches for the scan
        if ($valid) {
            $self->_parse_data($request, $port, $host, $acc, ($i+1), \%env_copy)
        }
    }
    return;
}

=head2 _build_data()
    Builds data output for a combination of scan parameters contained in $env.
    Returns exactly one data object per $env unless the produced data is invalidated.
=cut
sub _build_data {
    my ($self, $request, $port, $host, $env) = @_;

    # The data hash to return
    my %output = (node => $host);

    # Get raw data for the host
    my $data = $request->{raw}{data}{$port}{$host};

    # Flag to indicate whether the produced data is valid
    my $valid = 1;

    # Handle each wanted data element for metadata and values
    while (my ($name, $attr) = each(%{$request->{composite}{data}})) {

        # Skip the node value for backward compatibility
        next if ($name eq 'node');

        # Get the source for the data element
        my $source = $attr->{source};

        # Values derived from OID data
        if ($attr->{source_type} eq 'oid') {

            # Replace all vars in the source with the values from $env
            # This is a non-destructive replacement
            if (keys %$env) {
                $source = $source =~ s/(@{[join("|", keys %$env)]})/$env->{$1}/gr;
            }

            # Does our generated OID have a key in the raw data?
            if (exists($data->{$source})) {

                # Assign its value
                $output{$name} = $data->{$source}{value};

                # Assign time only once for the output hash
                unless (exists($output{time}) || !exists($data->{$source}{time})) {
                    $output{time} = $data->{$source}{time};
                }
            }
        }
        # Values derived from scans
        elsif ($attr->{source_type} eq 'scan') {
            $output{$name} = $env->{$source};
        }
        # Values derived from a defined constant
        elsif ($attr->{source_type} eq 'constant') {
            $output{$name} = $request->{composite}{constants}{$source};
        }

        # Verify the data value and apply any required matching and exclusion
        $valid = 0 unless ($self->_check_value(\%output, $name, $attr));
    }

    # Ensure that a value for "time" has been set
    unless (exists($output{'time'}) && defined($output{'time'})) {

        # Set time from $env where the only data with a time is from a scan
        if (exists($env->{TIME})) {
            $output{time} = $env->{TIME};
        }
        # If we still couldn't set the time, the data is invalid
        else {
            $valid = 0;
            $self->logger->error("[".$request->{composite}{name}."] Produced data without a time value!");
        }
    }

    # Returns the output hash if valid, otherwise returns nothing
    return ($valid) ? \%output : undef;
}


=head2 _check_value()
    Checks whether data for a specific value is valid.
    Applies require_match and invert_match functionality for the value.
    Initializes the value's key in the hash if it doesn't exist.
=cut
sub _check_value {
    my ($self, $data, $name, $attr) = @_;
        
    # Initialize the data element's value as undef if it does not already exist
    $data->{$name} = undef if (!exists($data->{$name}));

    # Check if the data element's value is required to match a pattern
    if (exists($attr->{'require_match'})) {

        # Does the value for the field match the given pattern?
        my $match  = $data->{$name} =~ $attr->{'require_match'};

        # Indicates that we should drop the data hash on match instead of keeping it
        my $invert = exists($attr->{'invert_match'}) ? $attr->{'invert_match'} : 0;

        # Cases where we remove the data hash:
        #   1. The pattern didn't match and we don't want to invert that
        #   2. The pattern did match, but we're removing matches due to inversion
        if ((!$match && !$invert) || ($match && $invert)) {
            return;
        }
    }
    return 1;
}


=head2 _convert_data()
    Applies conversionsto the host data before sending to TSDS
=cut
sub _convert_data {

    my $self    = shift;
    my $request = shift;

    my $composite      = $request->{composite};
    my $composite_name = $composite->{name};

    # Get the array of all conversions
    my $conversions = $composite->{'conversions'};

    $self->logger->debug("[$composite_name] Applying conversions to the data");

    # Iterate over the data array for each host
    for my $host (keys %{$request->{'data'}}) {

        # Skip and use empty results if host has no data
        if (!$request->{'data'}{$host} || (ref($request->{'data'}{$host}) ne ref([]))) {
            $self->logger->error("[$composite_name] No data array was generated for $host!");
            next;
        }

        # Skip the host if no functions are being applied to the hosts's data
        next unless ($conversions);

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

                    $self->logger->debug("[$composite_name] Function has the following definition and vars:");
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

                    $self->logger->debug("[$composite_name] Replacement has the follwing vars:");
                    $self->debug_dump(\%vars);
                }

                # Matches
                elsif ($conversion->{'type'} eq 'match') {

                    $target_pattern =~ s/\$\{\}/\$\{$target\}/g;

                    %vars = map { $_ => 1 } $target_pattern =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;

                    unless (scalar(keys %vars)) {
                        $target_pattern = $conversion->{'pattern'};
                    }

                    $self->logger->debug("[$composite_name] Match has the following pattern and vars:");
                    $self->debug_dump($target_pattern);
                    $self->debug_dump(\%vars);
                }

                # Apply the conversion for the target to each data object for the host
                for my $data (@{$request->{'data'}{$host}}) {

                    # Initialize all temporary conversion attributes for the data object
                    my $temp_def     = $target_def;
                    my $temp_this    = $target_this;
                    my $temp_with    = $target_with;
                    my $temp_pattern = $target_pattern;

                    # Skip if the target data element doesn't exist in the object
                    unless (exists $data->{$target}) {
                        $self->logger->debug("[$composite_name] Data element missing $target for conversion!");
                        next;
                    };

                    my $conversion_err = 0;

                    # Replace data variables in the definition with their value
                    for my $var (keys %vars) {

                        # The value associated with the var for the data object
                        my $var_value;

                        # Check if there is data for the var, then assign it
                        if (exists($data->{$var})) {

                            # Set the var's value to the one from the data when it's defined
                            if (defined($data->{$var})) {
                                $var_value = $data->{$var};
                            }
                            # Functions can have undefined values
                            # Anything else with an undefined var value is erroneous
                            elsif ($conversion->{type} ne 'function') {
                                $conversion_err++;
                                last;
                            }
                            else {
                                $var_value = '';
                            }
                        }
                        # If not, see if the var is a user-defined constant,
                        # then assign it
                        elsif (exists($request->{'constants'}{$var})) {
                            $var_value = $request->{'constants'}{$var};
                        }
                        # If the var isn't anywhere, flag a conversion err
                        # and end the loop
                        else {
                            $conversion_err++;
                            last;
                        }

                        # Functions
                        if ($conversion->{'type'} eq 'function') {

                            $var_value = '' if (!defined($var_value));

                            # Replace the var indicators with their value
                            $temp_def =~ s/\$\{$var\}/$var_value/g;
                            $self->logger->debug("[$composite_name] Applying function: $temp_def");
                        }
                        # Replacements
                        elsif ($conversion->{'type'} eq 'replace') {

                            # Replace the var indicators with their value
                            $temp_with =~ s/\$\{$var\}/$var_value/g;
                            $temp_this =~ s/\$\{$var\}/$var_value/g;

                            $temp_this = '' if (!defined($temp_this));

                            $self->logger->debug("[$composite_name] Replacing \"$temp_this\" with \"$temp_with\"");
                        }
                        # Matches
                        elsif ($conversion->{'type'} eq 'match') {

                            # Replace the var indicators with their value
                            $temp_pattern =~ s/\$\{$var\}/\Q$var_value\E/;
                            $self->debug_dump($temp_pattern);
                        }
                    }

                    # Don't send a value for the data if the conversion can't be completed as requested
                    if ($conversion_err) {
                        $data->{$target} = undef;
                        next;
                    }

                    # Store the converted value
                    my $new_value;

                    # Function application
                    if ($conversion->{'type'} eq 'function') {
                        # Use the FUNCTIONS object rpn definition
                        #$new_value = $_FUNCTIONS{'rpn'}(
                        $new_value = $self->_rpn_calc(
                            [$data->{$target}],
                            $temp_def, 
                            $conversion, 
                            $data, 
                            $request, 
                            $host
                        );
                        if (defined($new_value)) {
                            $self->logger->debug("[$composite_name] Resulting data for $target: $new_value");
                        }
                        else {
                            $self->logger->debug("[$composite_name] Resulting data for $target: undef");
                        }
                    }
                    # Replacement application
                    elsif ($conversion->{'type'} eq 'replace') {

                        # Handle undefined variables
                        # Can't replace something with undef value
                        # Can't perform replacement on undef value
                        if (!defined($temp_with) || !defined($data->{$target})) {
                            $new_value = $data->{$target};
                        }
                        # Replace the value and set the new value as the result
                        else {
                            $new_value = Data::Munge::replace(
                                $data->{$target}, 
                                $temp_this,
                                $temp_with,
                                $conversion->{'global'} ? 'g' : undef
                            );
                        }
                    }
                    # Match application
                    elsif ($conversion->{'type'} eq 'match') {

                        # Skip matching when the target data is undef
                        next unless (exists($data->{$target}) && defined($data->{$target}));

                        # Get parameters for the match operation
                        # $exclude = Inverse match of the pattern, excluding matching values
                        # $drop_targets = CSV string of fields to drop on a postive match/exclusion
                        # $update = Update the value to a capture group on match (default)
                        my $exclude      = exists($conversion->{'exclude'}) ? $conversion->{'exclude'} : 0;
			            my $drop_targets = exists($conversion->{'drop'}) ? $conversion->{'drop'} : '';
                        my $update       = exists($conversion->{'update'}) ? $conversion->{update} : 1;                     
                        $self->logger->debug(Dumper($data->{$target}));
                        
                        # Keep the original value for matches that shouldn't update it
                        my $original_value = $data->{$target};

                        # Does the value match the pattern?
                        my $match = $original_value =~ /$temp_pattern/;
                       
                        if ($match) {
                            # Set new value to the match or set to undef if excluding
                            $new_value = ($exclude != 1) ? $1 : undef;
                        }
                        else {
                            # Set new value to undef if it didn't match, or to the original value if excluding
                            $new_value = ($exclude != 1) ? undef : $original_value;
                        }

                        # Defined values mean a positive match or exclusion
                        if (defined($new_value)) {

                            # Drop any fields in the CSV string
                            for my $drop_target (split(/,/, $drop_targets)) {
                                $self->logger->debug(sprintf("Dropping %s", $drop_target));
                                delete($data->{$drop_target}) if exists($data->{$drop_target});
                            }   
                        }

                        # Reset the value unless it should be updated from a capture group
                        unless ($update) {
                            $new_value = $original_value;
                        }
                    }
                    # Drops
                    elsif ($conversion->{'type'} eq 'drop') {

                        $self->logger->debug("[$composite_name] Dropping composite field $target");
                        delete $data->{$target};
                        next;
                    }

                    if (defined $new_value) {
                        $self->logger->debug("[$composite_name] Set value for $target to $new_value");
                    }

                    # Assign the new value to the data target
                    $data->{$target} = $new_value;
                }
            }
        }

        # Replace metadata names with *name for TSDS
        for my $meta_name (keys %{$request->{'meta'}}) {
            for my $data (@{$request->{'data'}{$host}}) {
                if (exists $data->{$meta_name}) {
                    $data->{$request->{'meta'}->{$meta_name}} = delete $data->{$meta_name};
                }
            }
        }
    }

    $self->logger->debug("[$composite_name] Finished applying conversions to the data");
    #$self->debug_dump($request->{data});
}


sub _rpn_calc {
    my ($self, $vals, $operand, $fctn_elem, $val_set, $request, $host) = @_;

    for my $val (@$vals) {

        # As a convenience, we initialize the stack with a copy of $val on it already
        my @stack = ($val);

        # Split the RPN program's text into tokens (quoted strings,
        # or sequences of non-space chars beginning with a non-quote):
        my @prog;
        my $progtext = $operand;

        while (length($progtext) > 0) {
            $progtext =~ /^(\s+|[^\'\"][^\s]*|\'([^\'\\]|\\.)*(\'|\\?$)|\"([^\"\\]|\\.)*(\"|\\?$))/;
            my $x = $1;
            push @prog, $x if $x !~ /^\s*$/;
            $progtext = substr $progtext, length($x);
        }

        # Track errors by function token
        my %func_errors;

        # Now, go through the program, one token at a time:
        for my $token (@prog) {

            # Quoted Strings
            if ($token =~ /^[\'\"]/) {

                if ($token =~ /^\"/) {
                    $token =~ s/^\"(([^\"\\]|\\.)*)[\"\\]?$/$1/;
                }
                else {
                    $token =~ s/^\'(([^\'\\]|\\.)*)[\'\\]?$/$1/;
                }

                # Unescape escapes
                $token =~ s/\\(.)/$1/g;
                push @stack, $token;
            }
            # Decimal Numbers
            elsif ($token =~ /^[+-]?([0-9]+\.?|[0-9]*\.[0-9]+)$/) {
                push @stack, ($token + 0);
            }
            # Variables
            elsif ($token =~ /^\$/) {
                push @stack, $val_set->{substr $token, 1};
            }
            # Host Variable
            elsif ($token =~ /^\#/) {
                push @stack, $request->{'hostvar'}{$host}{substr $token, 1};
            }
            # Hostname
            elsif ($token eq '@') {
                push @stack, $host;
            }
            # Treat as a function
            else {
                if (!defined($_RPN_FUNCS{$token})) {
                    if (!$func_errors{$token}) {
                        $self->logger->error("[".$request->{composite}{name}."] RPN function $token not defined!");
                    }
                    $func_errors{$token} = 1;
                    next;
                }

                $_RPN_FUNCS{$token}(\@stack);
            }
        }

        # Return the top of the stack
        return pop @stack;
    }
}


# Turns truthy values to 1, falsy values to 0. Like K&R *intended*.
sub _bool_to_int {
    return (shift) ? 1 : 0;
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
        push @$stack, (defined($a) && defined($b)) ? eval{$a / $b;} : undef;
    },
    # dividend divisor => remainder
    '%' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? eval{$a % $b;} : undef;
    },
    # number => logarithm_base_e
    'ln' => sub {
        my $stack = shift;
        my $x     = pop @$stack;
        $x = eval{ log($x); }; # make ln(0) yield undef
        push @$stack, $x;
    },
    # number => logarithm_base_10
    'log10' => sub {
        my $stack = shift;
        my $x     = pop @$stack;
        $x = eval { log($x); }; # make ln(0) yield undef
        $x /= log(10) if defined($x);
        push @$stack, $x;
    },
    # number => power
    'exp' => sub {
        my $stack = shift;
        my $x     = pop @$stack;
        $x = eval{ exp($x); } if defined($x);
        push @$stack, $x;
    },
    # base exponent => power
    'pow' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? eval{$a ** $b;} : undef;
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
        #my $a_str = defined $a ? $a : 'undef';
        #my $b_str = defined $b ? $b : 'undef';
        #warn("RPN OPERATION: $a_str || $b_str");
        push @$stack, _bool_to_int($a || $b);
    },
    # a => (NOT a)
    'not' => sub {
        my $stack = shift;
        my $a     = pop @$stack;
        #my $a_str = defined($a) ? $a : 'undef';
        #warn("RPN OPERATION: !$a");
        push @$stack, _bool_to_int(!$a);
    },
    # pred a b => (a if pred is true, b if pred is false)
    'ifelse' => sub {
        my $stack = shift;
        my $b     = pop @$stack;
        my $a     = pop @$stack;
        my $pred  = pop @$stack;
        #my $a_str = defined $a ? $a : 'undef';
        #my $b_str = defined $b ? $b : 'undef';
        #my $p_str = defined $pred ? $pred : 'undef';
        #warn("RPN OPERATION: ($p_str) ? $a_str : $b_str");
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
        if (!defined($a) || ($a + 0) < 1) {
            push @$stack, undef;
            return;
        }
        push @$stack, $stack->[-($a + 0)];
        # This pushes undef if $a is greater than the stack size, which is OK
    },
);


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

1;
