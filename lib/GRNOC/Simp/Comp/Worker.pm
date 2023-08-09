package GRNOC::Simp::Comp::Worker;

use strict;
use warnings;

### REQUIRED IMPORTS ###
use JSON::Schema;

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
has config     => (is => 'ro', required => 1);
has rmq_config => (is => 'ro', required => 1);
has logger     => (is => 'ro', required => 1);
has composites => (is => 'ro', required => 1);
has worker_id  => (is => 'ro', required => 1);

### INTERNAL ATTRIBUTES ###
has is_running     => (is => 'rwp', default => 0);
has dispatcher     => (is => 'rwp');
has rmq_client     => (is => 'rwp');
has do_shutdown    => (is => 'rwp', default => 0);
has rmq_dispatcher => (is => 'rwp', default => sub { undef });

my %_RPN_FUNCS; # Used by _rpn_calc

### PUBLIC METHODS ###
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

    # NEW METHOD DEFINITION (Simp version 2+)
    my $method = GRNOC::RabbitMQ::Method->new(
        name        => 'get',
        async       => 1,
        callback    => sub { $self->_get(undef, @_); },
        description => 'Get data from Simp::Comp for one or multiple request parameter hashes'
    );
    # The "schema" arg is a necessary hack to allow passing non-atomic types ARRAY and HASH
    $method->add_input_parameter(
        name        => 'requests',
        description => 'Array or single Simp::Comp request parameter hash/hashes',
        schema      => JSON::Schema->new({properties => {requests => {type => 'any'}}}),
        required    => 1,
        multiple    => 0,
    );
    $self->rmq_dispatcher->register_method($method);

    # OLD METHOD DEFINITION (Simp version 1)
    # Register every composite as a RabbitMQ method calling _get() using its parameters
    while (my ($name, $composite) = each(%{$self->composites})) {

        # Method Definition
        my $old_method = GRNOC::RabbitMQ::Method->new(
            name        => "$name",
            async       => 1,
            callback    => sub { $self->_get($name, @_) },
            description => "Retrieve composite data of type \"$name\""
        );
        $old_method->add_input_parameter(
            name        => 'node',
            description => 'The desired array of nodes or single node',
            required    => 1,
            multiple    => 1,
            pattern     => $GRNOC::WebService::Regex::TEXT
        );
        $old_method->add_input_parameter(
            name        => 'period',
            description => "The desired period of time for rate calculations",
            required    => 0,
            multiple    => 0,
            pattern     => $GRNOC::WebService::Regex::ANY_NUMBER
        );
        $old_method->add_input_parameter(
            name        => 'time',
            description => "The desired time of the data to retrieve (default now())",
            required    => 0,
            multiple    => 0,
            pattern     => $GRNOC::WebService::Regex::ANY_NUMBER
        );
        $self->rmq_dispatcher->register_method($old_method);

        # DEPRECATED CODE
        # Process any input variables the composite has
        # my $inputs = [];
        # if (exists $composite->{'variables'}[0]{'input'}) {
        #     $inputs = $composite->{'variables'}[0]{'input'};
        # }
        # Allow composites to define custom input parameters for the method
        # my %predefined_param = map { $_ => 1 } ('node', 'period', 'exclude_regexp');
        # while ( my ($input, $attr) = each(%{$composite->{'inputs'}}) ) {
        #     unless ($predefined_param{$input}) {
        #         $method->add_input_parameter(
        #             name        => $input,
        #             description => "we will add description to the config file later",
        #             required    => exists $attr->{'required'} ? $attr->{'required'} : 1,
        #             multiple    => 1,
        #             pattern     => $GRNOC::WebService::Regex::TEXT
        #         );
        #     }
        # }
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
    my ($self, $composite, $rpc_ref, $args) = @_;

    # Track the time it took to process the request(s)
    my $start = [gettimeofday];

    # Initialize the requests array for _get to pass from method to method
    my $requests = $self->_init_requests($args, $composite);

    # RabbitMQ callback that sends our finished data
    my $success_callback = $rpc_ref->{'success_callback'};

    # Store results in this hash
    my %results;

    for my $request (@$requests) {

        # Initialize the AnyEvent conditional variables for the async processing pipeline
        my @cv = map { AnyEvent->condvar; } (0 .. 1);

        # Gather the raw SNMP data for every node and OID in scans or data elements from Redis
        $cv[0]->begin(sub { $self->_gather_data($request, $cv[1]); });
        
        # Wait until the data has been fully gathered before performing any steps
        $cv[1]->begin(sub {

            # Digest the raw data into arrays of data objects per node
            $self->_digest_data($request);

            # Apply conversions to the digested node data
            $self->_convert_data($request);
            
            $self->logger->error(Dumper($request->{'cache'}{'out'}));

            # Log the time to process the request 
            # $self->logger->info(sprintf("REQTIME COMP (%s requests): %.3fs", scalar(@$requests), tv_interval($start, [gettimeofday])));
            $self->logger->info(sprintf("REQTIME COMP: %.3fs", tv_interval($start, [gettimeofday])));

            # Add output data to the results.
            while (my ($node, $data) = each(%{$request->{'cache'}{'out'}})) {
                unless (exists($results{$node})) {
                    $results{$node} = $data;
                }
                else {
                    push(@{$results{$node}}, @$data);
                }
            }

            # my %results;
            # for my $request (@$requests) {

            #     my $cache = $request->{'cache'};

            #     while (my ($node, $data) = each(%{$cache->{'out'}})) {

            #         unless (exists($results{$node})) {
            #             $results{$node} = $data;
            #         }
            #         else {
            #             push(@{$results{$node}}, @$data);
            #         }
            #     }
            # }

            # Send results back to RabbitMQ for TSDS
            # &$success_callback(\%results);

            # Clear vars defined for the request
            # undef $requests;
            # undef %results;
            # undef $composite;
            # undef $args;
            # undef @cv;
            # undef $success_callback;
        });

        # Reset the async pipeline:
        $cv[0]->end;
        undef @cv;
    }
    # $self->logger->error(Dumper(\%results));

    &$success_callback(\%results);

    # Clear variables used in the requests from memory
    undef $requests;
    undef %results;
    undef $composite;
    undef $args;
    undef $success_callback;
}

# Validates required request parameters received from the queue.
sub _validate_request {
    my ($self, $request) = @_;
    return unless ($request->{'nodes'});
    return unless ($request->{'interval'});
    return unless ($request->{'composite'});
    return 1;
}

=head2 _init_requests()
    Initializes an array of request hashes containing all data for the _get request.
    The array is cleared after _get finishes sending back the requested data.
=cut
sub _init_requests {
    my ($self, $args, $composite) = @_;

    # The array of request hashes that will be returned
    my @requests;

    # Determine the version of the request format
    # Legacy requests pass the composite name separately
    my $legacy = ($composite) ? 1 : 0;

    # Legacy request handling for backward compatibility
    if ($legacy) {

        my %request = (
            composite => $composite,
            nodes     => $args->{'node'}{'value'},
            interval  => $args->{'period'}{'value'} || 60,
            time      => $args->{'time'}{'value'} || time(),
            legacy    => $legacy
        );

        # Create a cache for raw and finished output data for the request
        $request{'cache'} = {
            'scan' => {},
            'data' => {},
            'out'  => {map {$_ => []} @{$request{'nodes'}}},
        };

        # Only keep valid request hashes
        push(@requests, \%request) if ($self->_validate_request(\%request));

        # Return early to avoid nesting
        return \@requests;
    }

    # Modern request handling
    my $requested = $args->{'requests'}{'value'};

    # Individual request hashes are wrapped in an array for uniform processing
    $requested = [$requested] unless (ref($requested) eq 'ARRAY');

    # Create a request hash for every hash of request parameters
    for my $params (@$requested) {

        my %request = (
            composite => $params->{'composite'},
            nodes     => [$params->{'node'}],
            interval  => $params->{'interval'} || 60,
            time      => $params->{'time'} || time(),
            legacy    => $legacy
        );

        # Create a cache for raw and finished output data for the request
        $request{'cache'} = {
            'scan' => {},
            'data' => {},
            'out'  => {map {$_ => []} @{$request{'nodes'}}},
        };

        # Only keep valid request hashes
        push(@requests, \%request) if ($self->_validate_request(\%request));
    }

    return \@requests;
}


=head2 _gather_data()
    A method that will gather all OID data needed for the _get() request.
=cut
sub _gather_data {
    my ($self, $request, $cv) = @_;
    # $self->logger->debug(sprintf('Gathering data for %s requests', scalar(@$requests)));

    # for my $request (@$requests) {
        
        # Get the composite config for the request
        my $composite = $self->composites->{$request->{'composite'}};

        $self->logger->debug(sprintf('Getting %s data from Redis', $composite->{'name'}));

        # Gather data for scans
        for my $scan (@{$composite->{'scans'}}) {
            $self->_request_data($request, $scan->{'value'}, $scan, $cv);
        }

        # Gather data for data elements
        while (my ($name, $attr) = each(%{$composite->{'data'}})) {

            # Skip data elements where their source is a scan
            next if ($attr->{'source_type'} eq 'scan');

            # Skip data elements where their source is a constant
            next if ($attr->{'source_type'} eq 'constant');

            $self->_request_data($request, $name, $attr, $cv);
        }
    # }
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
    my $oid = $attr->{'base_oid'};

    # Add to the AnyEvent condition variable to make the request async
    # We end it within the callback after data caching completes
    $cv->begin;

    # Flag for rate calculations
    my $rate = exists($attr->{'type'}) && defined($attr->{'type'}) && $attr->{'type'} eq 'rate';

    # Requests for a calculated rate value
    if ($rate) {
        $self->rmq_client->get_rate(
            node           => $request->{'nodes'},
            period         => $request->{'interval'},
	        time           => $request->{'time'},
            oidmatch       => [$oid],
            async_callback => sub {
                my $data = shift;
                $self->_cache_data($request, $name, $attr, $data->{'results'});
                $cv->end;
            }
        );
    }
    # Requests for ordinary OID values
    else {
        $self->rmq_client->get(
            node           => $request->{'nodes'},
	        time           => $request->{'time'},
            oidmatch       => [$oid],
            async_callback => sub {
                my $data = shift;
                $self->_cache_data($request, $name, $attr, $data->{'results'});
                $cv->end;
            }
        );
    }
}


=head2 _cache_data()
    A callback used to cache raw OID data in the environment hash.
    Scans are cached in an array at $request->{cache}{scan}{$node}.
    Data are cached in a hash at $request->{cache}{data}{$node}.
    OIDs returned from Redis are checked to see if they match what was requested.
=cut
sub _cache_data {
    my ($self, $request, $name, $attr, $data) = @_;

    my $composite_name = $request->{'composite'};

    # Check whether Simp::Data retrieved anything from Redis
    if (!defined($data)) {
        $self->logger->error("[$composite_name] Simp::Data could not retrieve \"$name\" from Redis");
        return;
    }

    # Determine the type of the element, scan or data element
    # Data elements have a 'source' attribute
    my $type = exists $attr->{'source'} ? 'data' : 'scan';

    # Get a ref to the request's data cache for the data type
    my $cache = $request->{'cache'}{$type};

    # Cache data for node+port combinations returned from simp-data
    for my $node (@{$request->{'nodes'}}) {

        for my $port (keys(%$data)) {

            # Skip the node when there's no data for it
            if (!exists($data->{$port}{$node}) || !defined($data->{$port}{$node})) {
                $self->logger->error("[$composite_name] $node has no data to cache for \"$name\"");
                next;
            }

            # Raw scan data for a node needs to be wrapped in an array per individual scan
            my @scan_arr;

            for my $oid (keys %{$data->{$port}{$node}}) {

                my $value = $data->{$port}{$node}{$oid}{'value'};
                my $time  = $data->{$port}{$node}{$oid}{'time'};

                # Only OIDs that have a value and time are kept
                next unless (defined $value && defined $time);
            
                # Only OIDs with matching constant values and required OID vars present are kept
                next unless ($oid =~ $attr->{'regex'});

                # Scans are cached in a flat array
                if ($type eq 'scan') {

                    # Add the oid to the data to flatten for scans
                    $data->{$port}{$node}{$oid}{'oid'} = $oid;
                
                    # Push the scan data into the scan's array
                    push(@scan_arr, $data->{$port}{$node}{$oid});
                }
                # Data is cached in a flat hash
                else {
                    $cache->{$port}{$node}{$oid} = $data->{$port}{$node}{$oid};
                }
            }

            # Push the array of raw data for the scan
            if ($type eq 'scan') {

                # Initialize the port hash for nodes
                unless (exists($request->{$type}{$port})) {
                    $cache->{$port} = {};
                }
                # Initialize the array of scans for the node and port
                unless (exists $request->{raw}{$type}{$port}{$node}) {
                    $cache->{$port}{$node} = [];
                }

                # Get the scan's index to preserve scan ordering
                # Async responses from multiple simp-data workers can cause unordered scan data caching
                my $scan_index = $attr->{'index'};

                # Assign the scan data to the appropriate index
                $cache->{$port}{$node}[$scan_index] = \@scan_arr;
            }
        }
    }
}


=head2 _digest_data()
    Digests the raw data into arrays of data hashes per-node.
    This function wraps the data parser in a node loop.
=cut
sub _digest_data {
    my ($self, $request) = @_;

    # for my $request (@$requests) {

        $self->logger->debug("[".$request->{'composite'}."] Digesting raw data");

        # Get a reference to the request's data cache
        my $cache = $request->{'cache'};

        # Determine the unique ports used as the superset of ports returned for scans and data
        my %ports;
        @ports{keys(%{$cache->{'data'}})} = ();
        @ports{keys(%{$cache->{'scan'}})} = ();

        # Parse the data for each node and port
        for my $node (@{$request->{'nodes'}}) {
            for my $port (keys(%ports)) {

                # Skip the node if it has no data from sessions using the port
                next unless (
                    exists($cache->{'data'}{$port}{$node}) ||
                    exists($cache->{'scan'}{$port}{$node})
                );

                # Accumulate parsed data objects for the node+port to an array
                my @node_data;
                $self->_parse_data($request, $port, $node, \@node_data);

                # Add the accumulation to the data for the node
                push(@{$cache->{'out'}{$node}}, @node_data);
            }
        }
    # }
}

=head2 _parse_data()
    Parses the raw data and pushes complete data objects for a node into an array recursively.
    Scans are recursed through in order, then a data object is built using those scan parameters.
    We pass an environment hash to each recursion containing KVPs for scan poll_values and oid_suffixes.
    When scans depth is exhausted, we build the data for the combination by calling _build_data().
=cut
sub _parse_data {

    my ($self, $request, $port, $node, $acc, $i, $env) = @_;

    # Initialize $i and $env for the first iterations
    $i   = 0 if (!defined($i));
    $env = {'PORT' => $port, 'NODE' => $node} if ($i == 0);

    # Get the config for the composite
    my $composite = $self->composites->{$request->{'composite'}};

    # Get the config for the scan
    my $scans = $composite->{'scans'};
    my $scan  = $scans->[$i];

    # Once recursion hits a leaf, build data for the current environment
    if ($i > $#$scans) {
        my $output = $self->_build_data($request, $port, $node, $env);
        push(@$acc, $output) if ($output);
        return;
    }

    # The data for the scan
    my $data = $request->{'cache'}{'scan'}{$port}{$node}[$i];

    # The value name and match configs for the scan
    my $value_name  = $scan->{'value'};
    my $matches     = $scan->{'matches'};

    # Iterate over each data hash in the scan's array
    for (my $j = 0; $j <= $#$data; $j++) {

        my $datum = $data->[$j];
        my $oid   = $datum->{'oid'};

        # Create a copy of $env each time an OID changes
        my %env_copy = %$env;

        # Set the value name in $env to the value assigned to the OID
        $env_copy{$value_name} = $datum->{'value'};

        # Check if the OID matches the scan's regex and store any matching variables
        my @oid_vars = ($oid =~ $scan->{'regex'});

        # Skip non-matching OIDs
        next unless (@oid_vars);

        # Set time for $env from scan data in case we're only polling for scans
        if (!exists($env_copy{'TIME'}) || !defined($env_copy{'TIME'})) {
            $env_copy{'TIME'} = $datum->{'time'};
        }

        # Used for checking the validity of $env for the OID throughout parsing
        my $valid = 1;

        # Get EVERY variable node within the current OID
        for (my $var_index = 0; $var_index <= $#oid_vars; $var_index++) {

            my $var_name = $scan->{'oid_vars'}[$var_index];

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
            $self->_parse_data($request, $port, $node, $acc, ($i+1), \%env_copy)
        }
    }
    return;
}

=head2 _build_data()
    Builds data output for a combination of scan parameters contained in $env.
    Returns exactly one data object per $env unless the produced data is invalidated.
=cut
sub _build_data {
    my ($self, $request, $port, $node, $env) = @_;

    # The data hash to return
    my %output = (node => $node);

    # Get raw data for the node
    my $data = $request->{'cache'}{'data'}{$port}{$node};

    # Get a ref for the composite configuration
    my $composite = $self->composites->{$request->{'composite'}};

    # Flag to indicate whether the produced data is valid
    my $valid = 1;

    # Handle each wanted data element for metadata and values
    while (my ($name, $attr) = each(%{$composite->{'data'}})) {

        # Skip the node value for backward compatibility
        next if ($name eq 'node');

        # Get the source for the data element
        my $source = $attr->{'source'};

        # Values derived from OID data
        if ($attr->{'source_type'} eq 'oid') {

            # Replace all vars in the source with the values from $env
            # This is a non-destructive replacement
            if (keys %$env) {
                $source = $source =~ s/(@{[join("|", keys %$env)]})/$env->{$1}/gr;
            }

            # Does our generated OID have a key in the raw data?
            if (exists($data->{$source})) {

                # Assign its value
                $output{$name} = $data->{$source}{'value'};

                # Assign time only once for the output hash
                unless (exists($output{time}) || !exists($data->{$source}{time})) {
                    $output{time} = $data->{$source}{time};
                }
            }
        }
        # Values derived from scans
        elsif ($attr->{'source_type'} eq 'scan') {
            $output{$name} = $env->{$source};
        }
        # Values derived from a defined constant
        elsif ($attr->{'source_type'} eq 'constant') {
            $output{$name} = $composite->{'constants'}{$source};
        }

        # Verify the data value and apply any required matching and exclusion
        $valid = 0 unless ($self->_check_value(\%output, $name, $attr));
    }

    # Ensure that a value for "time" has been set
    unless (exists($output{'time'}) && defined($output{'time'})) {

        # Set time from $env where the only data with a time is from a scan
        if (exists($env->{'TIME'})) {
            $output{'time'} = $env->{'TIME'};
        }
        # If we still couldn't set the time, the data is invalid
        else {
            $valid = 0;
            $self->logger->error("[".$request->{'composite'}."] Produced data without a time value!");
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
    if (exists($attr->{'require_match'}) && defined($attr->{'require_match'})) {

        # Undefined data values cannot be matched, remove the hash
        return unless (defined($data->{$name}));

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
    Applies conversions to the node data before sending to TSDS
=cut
sub _convert_data {
    my ($self, $request) = @_;

    # for my $request (@$requests) {

        # Get the request's composite config
        my $composite_name = $request->{'composite'};
        my $composite      = $self->composites->{$composite_name};

        # Get a reference to the request's cache
        my $cache = $request->{'cache'};

        # Get the array of all conversions
        my $conversions = $composite->{'conversions'};

        $self->logger->debug("[$composite_name] Applying conversions to the data");

        # Iterate over the data array for each node
        #for my $node (keys %{$cache->{'out'}}) {
        while (my ($node, $node_data) = each(%{$cache->{'out'}})) {

            # Skip and use empty results if node has no data
            if (!$node_data || (ref($node_data) ne ref([]))) {
                $self->logger->error("[$composite_name] No data array was generated for $node!");
                next;
            }

            # Skip the node if no functions are being applied to the nodes' data
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

                    # Apply the conversion for the target to each data object for the node
                    for my $data (@$node_data) {

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
                                $node
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
                            my $update       = exists($conversion->{'update'}) ? $conversion->{'update'} : 1;                     
                            
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

            # Apply formatting to the final output data
            # if ($request->{'legacy'}) {
            #     $self->_legacy_formatting($request);
            # }
            # else {
            #     $self->_tsds_formatting($request);
            # }

            # Replace metadata names with *name for TSDS
            for my $data (@{$cache->{'out'}{$node}}) {
                while (my ($name, $attr) = each(%{$composite->{'data'}})) {
                    next unless ($attr->{'data_type'} eq 'metadata');
                    $data->{"*$name"} = delete $data->{$name} if (exists($data->{$name}));
                }
            }
        }

        $self->logger->debug("[$composite_name] Finished applying conversions to the data");
        #$self->debug_dump($request->{data});
    # }
}

# Formats data as flat hashes where metadata is prepended with "*"
sub _legacy_formatting {
    my ($self, $request, $node) = @_;

    my $composite = $self->composites->{$request->{'composite'}};
    my $cache     = $request->{'cache'};

    while (my ($name, $attr) = each(%{$composite->{'data'}})) {

        next if ($attr->{'data_type'} ne 'metadata');

        for my $data (@{$cache->{'out'}{$node}}) {

            if (exists $data->{$name}) {
                my $new_name = sprintf('*%s', $name);
                $data->{$new_name} = delete $data->{$name};
            }
        }
    }
}

sub _tsds_formatting {
    my ($self, $node, $request) = @_;
    my @output;
    for my $data (@{$request->{'data'}{$node}}) {
        my %meta;
        my %vals;
        my %data_message = (
            interval => $request->{'interval'},
            time     => $request->{'time'},
        );
        while (my ($k, $v) = each(%$data)) {
            $data_message{'meta'}{$k}   = $v if (exists($request->{'meta'}{$k}));
            $data_message{'values'}{$k} = $v if (exists($request->{'vals'}{$k}));
        }
        push(@output, \%data_message);
    }
    return \@output;
}

sub _rpn_calc {
    my ($self, $vals, $operand, $fctn_elem, $val_set, $request, $node) = @_;

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
                push @stack, $request->{'hostvar'}{$node}{substr $token, 1};
            }
            # Hostname
            elsif ($token eq '@') {
                push @stack, $node;
            }
            # Treat as a function
            else {
                if (!defined($_RPN_FUNCS{$token})) {
                    if (!$func_errors{$token}) {
                        $self->logger->error("[".$request->{'composite'}."] RPN function $token not defined!");
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
