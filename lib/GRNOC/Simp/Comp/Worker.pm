package GRNOC::Simp::Comp::Worker;

use strict;
use warnings;

### REQUIRED IMPORTS ###
use Moo;
use AnyEvent;
use Data::Dumper;
use Data::Munge;
use Time::HiRes qw(gettimeofday tv_interval);

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

    # Initialize the request hash for _get to pass from method to method
    my $request = $self->_init_request($args, $composite);

    # RabbitMQ callback that handles our final results
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
        $self->logger->info("REQTIME COMP " . tv_interval($start, [gettimeofday]));

        # Send results back to RabbitMQ for TSDS
        &$success_callback($request->{'final'});

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
    Initializes the request hash containing all data about a single _get request.
    The hash is cleared after _get finishes running.
=cut
sub _init_request {
    my $self      = shift;
    my $args      = shift;
    my $composite = shift;

    $self->logger->debug("Initializing the environment hash");

    # Create the request hash
    my %request = (
        composite => $composite,
        hosts     => $args->{node}{value},
        interval  => $args->{period}{value} || 60,
        final     => {},
        var_map   => {},
        constants => {},
        raw       => { scan => {}, data => {} },
        data      => {},
        meta      => {'node' => '*node'},
    );

    # Map any input variables for the composite
    if ($composite->{'inputs'}) {
        while ( my ($input, $attr) = each(%{$composite->{'inputs'}}) ) {
            $request{'var_map'}{$attr->{'value'}} = $input;
        }
    }

    # Map any constants for the composite
    if ($composite->{'constants'}) {
        while ( my ($const, $value) = each(%{$composite->{'constants'}}) ) {
            $request{'constants'}{$const} = $value;
        }
    }

    # Create host-specific hash entries
    for my $host (@{$request{hosts}}) {
        $request{data}{$host}      = [];
        $request{final}{$host}     = [];
        $request{raw}{scan}{$host} = [];
        $request{raw}{data}{$host} = {};
    }

    # Map scan poll_value and oid_suffix names
    for my $scan ( @{$composite->{scans}} ) {
        $request{'var_map'}{$scan->{value}} = $scan->{suffix};
    }

    # Map metadata names for TSDS
    for my $meta (keys %{$composite->{data}}) {
        my $attr = $composite->{data}{$meta};
        $request{'meta'}{$meta} = "*$meta" if ($attr->{data_type} eq 'metadata');
    }

    return \%request;
}


=head2 _gather_data()
    A method that will gather all OID data needed for the _get() request.
    The data is cached in the environment variable's "data" hash.
=cut
sub _gather_data {
    my $self    = shift;
    my $request = shift;
    my $cv      = shift;

    my $composite = $request->{composite};

    $self->logger->debug("Gathering data from Redis...");

    for my $scan (@{$composite->{'scans'}}) {
        
        $self->_request_data($request, $scan->{value}, $scan, $cv);
    }

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
    OID data is cached in $request->{data} by the callback.
=cut
sub _request_data {

    my $self    = shift;
    my $request = shift;
    my $name    = shift;
    my $attr    = shift;
    my $cv      = shift;

    # Get the base OID subtree to request from the element's map.
    # It is tempting to request just the OIDs you know you want,
    # instead of asking for the whole subtree, but posting a
    # bunch of requests for individual OIDs takes significantly
    # more time and CPU. That's why we request the subtree.
    $self->logger->debug("[$name] Requesting data");

    my $oid = $attr->{map}{base_oid};

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
=cut
sub _cache_data {
    my $self    = shift;
    my $request = shift;
    my $name    = shift;
    my $attr    = shift;
    my $data    = shift;

    # Check whether Simp::Data retrieved anything from Redis
    if (!defined($data)) {
        $self->logger->error("[$name] Could not retrieve data from Redis using Simp::Data");
        return;
    }

    # Get the OID map
    my $map = $attr->{map};

    # Determine the type of the element, scan or data element
    # Data elements have a 'source' attribute
    my $type = exists $attr->{source} ? 'data' : 'scan';

    for my $host (@{$request->{hosts}}) {

        # Skip the host when there's no data for it
        if (!exists($data->{$host}) || !defined($data->{$host})) {
            $self->logger->error("[$name] $host has no data to cache for this");
            next;
        }

        # For scans, we want an ordered array of arrays where the outer array indicates which scan we want
        my @output;

        # Check all of the OID data returned for the host
        for my $oid (keys %{$data->{$host}}) {

            my $value = $data->{$host}{$oid}{value};
            my $time  = $data->{$host}{$oid}{time};

            # Only OIDs that have a value and time are kept
            next unless (defined $value && defined $time);
            
            # Only OIDs with matching constant values and required OID vars present are kept
            next unless ($oid =~ $map->{regex});

            # Scans are cached in a flat array
            if ($type eq 'scan') {

                # Add the oid to the data to flatten for scans
                $data->{$host}{$oid}{oid} = $oid;
                
                # Push the scan data
                push(@{$request->{raw}{$type}{$host}}, $data->{$host}{$oid});
            }
            # Data is cached in a flat hash
            else {
                $request->{raw}{$type}{$host}{$oid} = $data->{$host}{$oid};
            }

        }
    }

    $self->logger->debug("[$name] Successfully cached data");
}


=head2 _digest_data()
    
=cut
sub _digest_data {
    my ($self, $request, $cv) = @_;

    # Parse the data for each host
    for my $host (@{$request->{hosts}}) {

        # Accumulate the host data then assign it
        my @host_data;
        $self->_parse_data($request, $host, \@host_data);
        $request->{data}{$host} = \@host_data;
    }
}

=head2 _parse_data()

=cut
sub _parse_data {

    my ($self, $request, $host, $acc, $i, $env) = @_;

    # Initialize $i and $env for the first iterations
    $i   = 0 if (!defined($i));
    $env = {} if ($i == 0);

    # Get the config and OID map for the scan
    my $scans = $request->{composite}{scans};
    my $scan  = $scans->[$i];
    my $map   = $scan->{map};

    # Once recursion hits a leaf, build data for the current environment
    if ($i > $#$scans) {
        my $output = $self->_build_data($request, $host, $env);
        push(@$acc, $output) if ($output);
        return;
    }

    # The data for the scan
    my $data = $request->{raw}{scan}{$host};

    # The name of this scan's poll_value and oid_suffix
    my $value_name  = $scan->{value};
    my $suffix_name = $scan->{suffix};

    # Iterate over the raw data for the host
    for (my $j = 0; $j <= $#$data; $j++) {

        my $datum = $data->[$j];
        my $oid   = $datum->{oid};

        # Check if the OID matches the scan's regex and store any matching variables
        my @oid_vars = ($oid =~ $map->{regex});
        
        # Skip non-matching OIDs
        next unless (@oid_vars);

        # Set time for $env from scan data in case we're only polling for scans
        if (!exists($env->{TIME}) || !defined($env->{TIME})) {
            $env->{TIME} = $datum->{time};
        }

        # Short circuit for elements without vars
        if (!defined($map->{first_var})) {
            $env->{$suffix_name} = undef;
            $env->{$value_name}  = $datum->{value};

            $self->_parse_data($request, $host, $acc, ($i+1), $env);

            next;
        }

        # Flag whether the OID has the right vars for what's in $env
        my $valid = 1;

        # Find the value for this scan data's OID suffix
        for (my $index = 0; $index <= $#oid_vars; $index++) {
            
            # Grab the var name and value from the map and data OID
            my $var = $map->{oid_vars}[$index];
            my $val = $oid_vars[$index];

            # Validate the var->val combination by checking this:
            #   The current var exists in $env... 
            #   and isn't for the current scan...
            #   and the var's value in $env matches the current OID's value for it
            $valid = 0 if (
                exists($env->{$var}) 
                && ($var ne $value_name && $var ne $suffix_name) 
                && ($env->{$var} ne $val)
            );    

            # Skip vars that aren't the current scan's suffix
            next if ($var ne $suffix_name);

            # Set the suffix and value for the scan data to recurse on
            if ($valid) {

                my %new_env = %$env;
                $new_env{$suffix_name} = $val;
                $new_env{$value_name}  = $datum->{value};

                # Once we have the suffix, we're done with the vars
                $self->_parse_data($request, $host, $acc, ($i+1), \%new_env);

                # Stop iterating over the OID variables for this particular OID
                last;
            }
        }
    }
    return;
}

=head2 _build_data()
    Builds data output for a combination of OID variables from scanning
=cut
sub _build_data {
    my ($self, $request, $host, $env) = @_;

    # The data hash to return
    my %output = (node => $host);

    # Get raw data for the host
    my $data = $request->{raw}{data}{$host};

    # Flag to indicate whether the produced data should be kept
    my $keep = 1;

    # Handle each wanted data element for metadata and values
    while (my ($name, $attr) = each(%{$request->{composite}{data}})) {

        $self->logger->debug("Processing $name for combination");

        # Skip the node for backward compatibility
        next if ($name eq 'node');

        my $map    = $attr->{map};
        my $source = $attr->{source};

        # Handling for OID sources
        if ($attr->{source_type} eq 'oid') {

            # Replace all vars in the source with values from $env
            for my $var (keys %$env) {
                my $val = $env->{$var};
                $source = $source =~ s/$var/$val/gr;
            }

            # Does our generated OID have a key in the raw data?
            if (exists($data->{$source})) {

                # Assign its value
                $output{$name} = $data->{$source}{value};

                # Add the time only once
                unless (exists($output{time}) || !exists($data->{$source}{time})) {
                    $output{time} = $data->{$source}{time};
                }
            }
        }
        # Handling for values derived from scans
        elsif ($attr->{source_type} eq 'scan') {
            $output{$name} = $env->{$source};
        }
        elsif ($attr->{source_type} eq 'constant') {
            $output{$name} = $request->{composite}{constants}{$source};
        }

        # Verify the data and apply any matches and match exclusions
        $keep = 0 unless ($self->_check_data(\%output, $name, $attr));
    }

    # Ensure that a value for "time" has been set
    unless (exists($output{'time'}) && defined($output{'time'})) {

        # Set time from $env where the only data with time is from a scan
        if (exists($env->{TIME})) {
            $output{time} = $env->{TIME};
        }
        else {
            $self->logger->error("ERROR: Simp.Comp Worker has data without a time. This shouldn't be possible!");
        }
    }

    return \%output if ($keep);
    return;
}


sub _check_data {
    my ($self, $data, $name, $attr) = @_;
        
    # Initialize the data element's value as undef if it does not already exist
    $data->{$name} = undef if (!exists($data->{$name}));

    # Check if the data element's value is required to match a pattern
    if (exists($attr->{'require_match'})) {

        # Does the value for the field match the given pattern?
        # If so, we keep the data object (unless inverted)
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
    Applies conversions to the host data before sending to TSDS
=cut
sub _convert_data {

    my $self    = shift;
    my $request = shift;    # Global evironment hash
    my $cv      = shift;    # AnyEvent condition var (assumes it's been begin()'ed)

    my $composite = $request->{composite};

    # Get the array of all conversions
    my $conversions = $composite->{'conversions'};

    $self->logger->debug("Applying conversions to the data");

    # Iterate over the data array for each host
    for my $host (keys %{$request->{'data'}}) {

        # Skip and use empty results if host has no data
        if (!$request->{'data'}{$host} || (ref($request->{'data'}{$host}) ne ref([]))) {
            $self->logger->error("ERROR: No data array was generated for $host!");
            next;
        }

        # Set final values if no functions are being applied to the hosts's data
        unless ($conversions) {
            $request->{'final'}{$host} = $request->{'data'}{$host};
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
                for my $data (@{$request->{'data'}{$host}}) {

                    # Initialize all temporary conversion attributes
                    # for the data object
                    my $temp_def     = $target_def;
                    my $temp_this    = $target_this;
                    my $temp_with    = $target_with;
                    my $temp_pattern = $target_pattern;

                    # Skip if the target data element doesn't exist
                    # in the object
                    next unless (exists $data->{$target});# && defined $data->{$target});

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
                            $temp_def =~ s/\$\{$var\}/$var_value/;
                            $self->logger->debug("Applying function: $temp_def");
                        }

                        # Replacements
                        elsif ($conversion->{'type'} eq 'replace') {

                            # Replace the var indicators with their value
                            $temp_with =~ s/\$\{$var\}/$var_value/g;
                            $temp_this =~ s/\$\{$var\}/$var_value/g;

                            $temp_this = '' if (!defined($temp_this));

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
                        #$new_value = $_FUNCTIONS{'rpn'}(
                        $new_value = $self->_rpn_calc(
                            [$data->{$target}],
                            $temp_def, 
                            $conversion, 
                            $data, 
                            $request, 
                            $host
                        );
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
                                $temp_with
                            );
                        }
                    }

                    # Match application
                    elsif ($conversion->{'type'} eq 'match') {

                        # Change the output of the match where exclusion on match is desired
                        my $exclude = exists($conversion->{'exclude'}) ? $conversion->{'exclude'} : 0;

                        # Set new value to pattern match or assign as undef
                        if ($data->{$target} =~ /$temp_pattern/) {
                            $new_value = ($exclude != 1) ? $1 : undef;
                        }
                        else {
                            $new_value = ($exclude != 1) ? undef : $data->{$target};
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

        # Replace metadata names with *name for TSDS
        for my $meta_name (keys %{$request->{'meta'}}) {
            for my $data (@{$request->{'data'}{$host}}) {
                if (exists $data->{$meta_name}) {
                    $data->{$request->{'meta'}->{$meta_name}} = delete $data->{$meta_name};
                }
            }
        }

        # Once all conversions are applied for the host, set the final data to that hash
        $request->{'final'}{$host} = $request->{'data'}{$host};
    }

    $self->debug_dump($request->{'final'});
    $self->logger->debug("Finished applying conversions to the data");
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
                    $self->logger->error("RPN function $token not defined!") if !$func_errors{$token};
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
