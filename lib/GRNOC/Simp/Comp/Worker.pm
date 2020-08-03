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
            description => "retrieve composite data of type $composite_name, we should add a descr to the config"
        );

        $method->add_input_parameter(
            name        => 'node',
            description => 'nodes to retrieve data for',
            required    => 1,
            multiple    => 1,
            pattern     => $GRNOC::WebService::Regex::TEXT
        );

        $method->add_input_parameter(
            name        => 'period',
            description => "period of time to request for the data!",
            required    => 0,
            multiple    => 0,
            pattern     => $GRNOC::WebService::Regex::ANY_NUMBER
        );

        $method->add_input_parameter(
            name        => 'exclude_regexp',
            description => 'a set of var=regexp pairs, where if scan variable var matches the regexp, we exclude it from the results',
            required    => 0,
            multiple    => 1,
            pattern     => '^([^=]+=.*)$'
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

    ### SUMMARY ###

    This is the primary method invoked when TSDS requests data
    It will run through retrieving data from Redis, processing, and sending the result


    ### PROCESS OVERVIEW ###

    Processes within simp-comp work asynchronously.
    A series of callbacks and functions use the $cv[*] AnyEvent condition variables
    to signal the beginning and end of a process, triggering the next process when ready.
    
    --- Here is the process order:
        1. _get_scans -> _scan_cb / Processes the scan configs and then gets their data via callback
        2. _digest_scans          / Combines the scan data for all scans into a single OID tree
        3. _get_data  -> _data_cb / Processes the val configs and then gets their data via callback
        4. _digest_data           / Builds data objects from all val data and then extracts them from the tree into a flat array
        5. _do_conversions        / Applies functions specified in configs to their val data

    
    ### DATA STRUCTURE ###
    
    Data is accumulated in a global %results hash:
    
    --- Results from the _get_scans -> _scan_cb -> _digest_scans phase:
    $results{'scan_tree'}{$node}{$scan_id} = { OID element tree returned from scans with empty leaves }
    $results{'scan_vals'}{$node}{$scan_id} = { OID element tree returned from scans with hashes of their values }
    
    This is from legacy code, no performance is gained with it. Leaving if hidden meaning is revealed...
    $results{'scan_exclude'}{$node}{$oid} = 1 (Any value here marks an oid to be excluded)
    
    --- Results from the _get_data -> _data_cb -> _digest_data phase:
    $results{'data'}{$host}{$val_id} = { OID element tree of hashes containting data returned for the val }
    $results{'hostvar'}{$host}{$hostvar_name} = The host variables (_get_data and _hostvar_cb)
    
    --- Results from the _do_conversions phase:
    $results{'final'}{$host} = [ Array of all data objects for the host that is passed back to the caller ]

=cut
sub _get {

    my $start     = [gettimeofday];
    my $self      = shift;
    my $composite = shift;
    my $rpc_ref   = shift;
    my $params    = shift;

    if (!defined($params->{'period'}{'value'})) {
        $params->{'period'}{'value'} = 60;
    }

    # Initialize the results hash for _get to pass from method to method
    my %results;

    # Make sure the final results hash exists, even if we get zero results
    $results{'final'} = {};

    # Initialize the variables map and add any inputs to it
    $results{'var_map'} = {};

    # Process any input variables the composite has
    if ($composite->{'inputs'}) {
        while ( my ($input, $attr) = each(%{$composite->{'inputs'}}) ) {
            $results{'var_map'}{$attr->{'value'}} = $input;
        }
    }

    # Initialize any configured constants for the composite
    $results{'constants'} = {};
    if ($composite->{'constants'}) {
        while ( my ($const, $value) = each(%{$composite->{'constants'}}) ) {
            $results{'constants'}{$const} = $value;
        }
    }

    my $success_callback = $rpc_ref->{'success_callback'};

    # Initialize the AnyEvent condition vars for the processing pipeline
    my @cv = map { AnyEvent->condvar; } (0 .. 5);

    # Step through data processing asynchronously using AnyEvent
    $cv[0]->begin(sub { $self->_get_scans($composite, $params, \%results, $cv[1]); });
    $cv[1]->begin(sub { $self->_digest_scans($params, \%results, $cv[2]); });
    $cv[2]->begin(sub { $self->_get_data($composite, $params, \%results, $cv[3]); });
    $cv[3]->begin(sub { $self->_digest_data($composite, $params, \%results, $cv[4]); });
    $cv[4]->begin(sub { $self->_do_conversions($composite, $params, \%results, $cv[5]); });
    $cv[5]->begin(
        sub {
            my $end = [gettimeofday];
            my $resp_time = tv_interval($start, $end);
            $self->logger->info("REQTIME COMP $resp_time");
            &$success_callback($results{'final'});
            undef %results;
            undef $composite;
            undef $params;
            undef @cv;
            undef $success_callback;
        }
    );

    # Reset the async pipeline:
    $cv[0]->end;
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


# Polls for the scan elements of a composite
sub _get_scans {

    my $self      = shift;
    my $composite = shift;    # The instance XPath from the config
    my $params    = shift;    # Parameters to request
    my $results   = shift;    # Global $results hash
    my $cv        = shift;    # AnyEvent condition var (assumes it's been begin()'ed)

    $self->logger->debug("Running _get_scans");

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    # find the set of exclude patterns, and group them by var
    my %exclude_patterns;
    for my $pattern (@{$params->{'exclude_regexp'}{'value'}}) {
        $pattern =~ /^([^=]+)=(.*)$/;
        push @{$exclude_patterns{$1}}, $2;
    }

    #--- This function will execute multiple scans in "parallel" using the begin/end approach
    #--- We use $cv to signal when all those scans are done
    #--- these should be moved to the constructor

    # Make sure several root hashes exist
    for my $host (@$hosts) {

        $results->{'scan_tree'}{$host} = {};
        $results->{'scan_vals'}{$host} = {};
        $results->{'data'}{$host}      = {};
        $results->{'hostvar'}{$host}   = {};
    }

    my $scans = $composite->{'scans'};

    while ( my ($scan, $attr) = each(%$scans) ) {

        my $suffix = $attr->{'suffix'};
        my $oid    = $attr->{'oid'};

        # Example Scan:
        # <scan oid_suffix="ifIdx" poll_value="ifName" oid="1.3.6.1.2.1.31.1.1.1.18.*" />

        # Add the scan to the var map
        $results->{'var_map'}->{$scan} = $suffix;

        # Ensure that a map exists for the scan
        if (!defined $scans->{$scan}->{'map'}) {
            $self->logger->error("ERROR: no map exists for scan \"$scan\"!");
            next;
        }

        # Get the base oid to poll, (defaults to the original OID if no vars)
        my $base_oid = $attr->{'map'}{'base_oid'};

        # Callback to get the polling data for the base oid
        # of the scan into results
        $cv->begin;
        $self->rmq_client->get(
            node           => $hosts,
            oidmatch       => $base_oid,
            async_callback => sub {
                my $data = shift;
                $self->_scan_cb($data->{'results'}, $hosts, $results, $scan, $attr);
                $cv->end;
            }
        );
    }

    $self->logger->debug("Completed _get_scans");

    # Signals _digest_scans to start when all callbacks complete
    $cv->end;
}


# Gets data for a scan for the OIDs we want to scan
# and sets scan_tree and scan_vals
sub _scan_cb {

    my $self    = shift;
    my $data    = shift;
    my $hosts   = shift;
    my $results = shift;
    my $scan    = shift;
    my $attr    = shift;

    my $scan_map    = $attr->{'map'};
    my $scan_suffix = $attr->{'suffix'};
    my $scan_oid    = $attr->{'oid'};
    my $targets     = $attr->{'targets'};
    my $excludes    = $attr->{'excludes'};

    # True = Add no results, but possibly blacklist OID values
    my $exclude_only = $attr->{'ex_only'};

    $self->logger->debug("Running _scan_cb for $scan_suffix");

    for my $host (@$hosts) {
        
        if (!$data->{$host}) {
            $self->logger->error("No scan data could be retrieved for $host in callback for $scan_oid");
            next;
        }

        # Track the OIDs we want after exclusions and targets are factored in
        my @oids;

        # Return only entries matching specified value regexps,
        # if value regexps are specified
        my $has_targets = (defined($targets) && (scalar(@$targets) > 0));

        # Check our oids and keep only the ones we want
        for my $data_oid (keys %{$data->{$host}}) {

            my $oid_value = $data->{$host}{$data_oid}{'value'};

            # Blacklist the value if it matches an exclude
            next if (any { $oid_value =~ /$_/ } @$excludes);

            # Skip the OID if the host is exclusion only and
            # is using target matches or the value matches a target
            if ($exclude_only) {
                next unless (!$has_targets || (any { $scan_oid =~ /$_/ } @$targets));
            }
            # If we didn't exclude the oid, add it to our OIDs to translate
            else {
                push @oids, $data_oid;
            }
        }

        $self->logger->debug(scalar(@oids) . " OIDs were pushed for transformation");

        # Add the data for the scan to results if we're not excluding the host
        if (!$exclude_only && scalar(@oids)) {

            # Transform the OID data into an OID tree with empty leaves to fill
            my $scan_transform = $self->_transform_oids(\@oids, $data->{$host}, $scan_map, 'scan');

            # Set scan_tree with the blank OID tree and legend from the scan
            $results->{'scan_tree'}{$host}{$scan_suffix}{'vals'}   = $scan_transform->{'blanks'};
            $results->{'scan_tree'}{$host}{$scan_suffix}{'legend'} = $scan_transform->{'legend'};

            # Set scan_vals to the tree of values from the scan
            $results->{'scan_vals'}{$host}{$scan_suffix} = $scan_transform->{'vals'};
        }
    }

    $self->logger->debug("Finished running _scan_cb for $scan_suffix");
}


# Recursively combine the OID tree for a scan with one of another scan
sub _combine_scans {

    my $self     = shift;
    my $scan     = shift;
    my $combined = shift;

    return if (!scalar(%{$scan}));

    for my $key (keys %{$scan}) {
        if (!exists $combined->{$key}) {
            $combined->{$key} = {};
        }
        else{
            $self->_combine_scans($scan->{$key}, $combined->{$key});
        }
    }
}


# Process and combine the scan results once all of the scans and their callbacks have completed
sub _digest_scans {

    my $self    = shift;
    my $params  = shift;    # Parameters to request
    my $results = shift;    # Request-global $results hash
    my $cv      = shift;    # Assumes that it's been begin()'ed with a callback

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    $self->logger->debug("Digesting combined scans");

    for my $host (@$hosts) {

        my %combined_scan;

        # Get the scans for the host
        my $scans = $results->{'scan_tree'}{$host};

        if (scalar(keys %{$scans}) < 1) {
            $self->logger->error("There is no scan data for $host!");
            next;
        }
        else {
            $self->logger->debug("_digest_scans found data for ". scalar(keys %{$scans}). " scans");
        }

        # Single scans don't need to be combined;
        if (scalar(keys %{$scans}) < 2) {
            $self->logger->debug("Single scan found, using that scan");

            for my $scan_id (keys %{$scans}) {
                $results->{'scan_tree'}{$host} = $results->{'scan_tree'}{$host}{$scan_id};
                last;
            }

            next;
        }

        # Otherwise, combine the dependent scan results for the host
        else {

            my @main_legend;
            my $main_scan;

            # Find the scan with the most dependencies and
            # use its legend and scan val tree
            for my $scan (keys %{$scans}) {

                my $legend = $scans->{$scan}{'legend'};

                if (!@main_legend || $#main_legend < $#$legend) {
                    @main_legend = @{$legend};
                    $main_scan   = $scan;
                }
            }

            # Use our main legend and vals for the main scan as a base
            # to combine parent scans with
            if (@main_legend && defined $main_scan) {
                $combined_scan{'legend'} = \@main_legend;
                $combined_scan{'vals'}   = $scans->{$main_scan}{'vals'};
            }
            else {
                $self->logger->error("No legend was found for any scans");
                return;
            }

            # Loop over the parent scans, combining them into one OID tree
            for (my $i = 0; $i < $#main_legend; $i++) {
                my $scan = $scans->{$main_legend[$i]}{'vals'};
                $self->_combine_scans($scan, $combined_scan{'vals'});
            }
        }

        # Replace our scanned OID trees for the host with one combined one
        $results->{'scan_tree'}{$host} = \%combined_scan;
    }

    $self->logger->debug("Finished digesting scans");

    # trigger the _get_data callback
    $cv->end;
}


# Fetches the host variables and SNMP values for <val> elements
sub _get_data {

    my $self      = shift;
    my $composite = shift;    # The instance XPath from the config
    my $params    = shift;    # Parameters to request
    my $results   = shift;    # Global $results hash
    my $cv        = shift;    # AnyEvent condition var (assumes it's been begin()'ed)

    $self->logger->debug("Running _get_data");

    # Get the set of required variables
    my $hosts = $params->{'node'}{'value'};

    # This callback does multiple gets in "parallel" using the begin/end apprach
    # $cv is used to signal when the gets are done
    $cv->begin;
    $self->rmq_client->get(
        node           => $hosts,
        oidmatch       => 'vars.*',
        async_callback => sub {
            my $data = shift;
            $self->_hostvar_cb($data->{'results'}, $results);
            $cv->end;
        },
    );

    while ( my ($name, $attr) = each(%{$composite->{'data'}}) ) {

        # Add metadata names to a map with the name as an asterisk
        # The asterisk indicates a metadata field in TSDS
        # We use the map to rename the metadata field before sending
        if ($attr->{'data_type'} eq 'metadata') {
            $results->{'meta'}->{$name} = "*$name";
        }

        # Attributes for <meta> and <value> Elements --------------------------------------------
        #
        #   "name" (required):
        #       - Should be the name of a metadata or value field from the TSDS measurement type.
        #       - The use of a meta or value tag determines if a field is metadata or a value.
        #
        #   "source" (required):
        #       - Has four possible options:
        #           1. The poll_value name of a scan from the variables.
        #           2. The name of an input from the variables.
        #           3. A variable OID that uses oid_suffixes from scans.
        #           4. A regular OID that is static and returns one result.
        #
        #   "type" (optional):
        #       - Must be set equal to "rate", it is the only option right now.
        #       - This is used to indicate that a numeric value should be calculated as a rate.
        #       - Typically used for bit and packet rates that need a difference calculation.
        #
        #----------------------------------------------------------------------------------------

        # Handling for data elements where the source derives its values from a scan
        if ($attr->{'source_type'} eq 'scan') {

            # If the val is the "node" var, create a val object in
            # results with one value set to the host
            if ($name eq 'node') {
                for my $host (@$hosts) {
                    $results->{'data'}{$host}{'node'}{'value'} = $host;
                }
            }
            # If the element points to a constant use the value of the constant
            elsif (exists $results->{'constants'}{$attr->{'source'}}) {
                for my $host (@$hosts) {
                    $results->{'data'}{$host}{$name}{'value'} = $results->{'constants'}{$attr->{'source'}};
                }
            }
            else {

                # Add the scan_vals hash for it to the results
                # val hash under its val ID
                my $val_key = $results->{'var_map'}->{$attr->{'source'}};

                for my $host (@$hosts) {

                    # Assign the OID suffixes as the value
                    if ( exists $results->{'scan_vals'}{$host}{$attr->{'source'}}) {

                        $self->logger->debug("Getting data for source $attr->{'source'} from OID suffixes");

                        my $suffix_data = $results->{'scan_vals'}{$host}{$attr->{'source'}};

                        for my $key (keys %$suffix_data) {
                            $results->{'data'}{$host}{$name}{$key}{'value'} = $suffix_data->{$key}{'suffix'};
                        }
                    }
                    # Assign the poll_value as the value
                    elsif ($val_key && exists $results->{'scan_vals'}{$host}{$val_key}) {

                        $self->logger->debug("Getting data for source $attr->{'source'} from the $val_key values");
                        
                        $results->{'data'}{$host}{$name} = $results->{'scan_vals'}{$host}{$val_key};
                    }

                    else {
                        $self->logger->error("The data source $attr->{'source'} did not have any data for assignment!");
                    }
                }
            }
        }
        # Handling for data elements where the source is an absolute OID or OID with variable OID nodes
        elsif ($attr->{'source_type'} eq 'oid') {
         
            if (!defined $attr->{'map'}) {
                $self->logger->error("A map was not generated for data element \"$name\"!");
                next;
            }

            # Add the element's name to the val_map
            $attr->{'map'}{'name'} = $name;

            # Get the base OID of the val for polling
            my $base_oid = $attr->{'map'}{'base_oid'};

            for my $host (@$hosts) {

                if (scalar(keys %{$results->{'scan_tree'}{$host}}) < 1) {
                    $self->logger->error("ERROR: No scan data! Skipping vals for $host");
                    next;
                }

                # Get the data for these OIDs from Simp
                $cv->begin;

                # It is tempting to request just the OIDs you know you want,
                # instead of asking for the whole subtree, but requesting
                # a bunch of individual OIDs takes SimpData a *whole* lot
                # more time and CPU, so we go for the subtree.
                if (defined($attr->{'type'}) && $attr->{'type'} eq 'rate') {
                    $self->rmq_client->get_rate(
                        node           => [$host],
                        period         => $params->{'period'}{'value'},
                        oidmatch       => [$base_oid],
                        async_callback => sub {
                            my $data = shift;
                            $self->_data_cb($data->{'results'}, $results, $host, $attr->{'map'});
                            $cv->end; 
                        }
                    );

                }
                else {
                    $self->rmq_client->get(
                        node           => [$host],
                        oidmatch       => [$base_oid],
                        async_callback => sub {
                            my $data = shift;
                            $self->_data_cb($data->{'results'}, $results, $host, $attr->{'map'});
                            $cv->end;
                        }
                    );
                }
            }
        }
        # Handles data elements where the source type is invalid or doesn't exist
        else {
            $self->logger->error('ERROR: _get_data() received a data element with an invalid source type');
        }
    }

    $self->logger->debug("Finished running _get_data");

    # trigger the _digest_data callback
    $cv->end;
}


# Not sure what the point of this is, perhaps to queue data?
sub _hostvar_cb
{
    my $self    = shift;
    my $data    = shift;
    my $results = shift;

    $self->logger->debug("Running _hostvar_cb");

    for my $host (keys %$data) {
        for my $oid (keys %{$data->{$host}}) {
            my $val = $data->{$host}{$oid}{'value'};
            $oid =~ s/^vars\.//;
            $results->{'hostvar'}{$host}{$oid} = $val;
        }
    }

    $self->logger->debug("Finished running _hostvar_cb");
}


# Callback to get data for a val
sub _data_cb
{
    my $self     = shift;
    my $data     = shift;
    my $results  = shift;
    my $host     = shift;
    my $elem_map = shift;

    # Get the scan data for the host
    my $scan_data = $results->{'scan_tree'}{$host};

    $self->logger->debug("Scan data found for _data_cb: " . scalar(keys %{$scan_data}));

    $self->logger->debug("Running _data_cb");

    # Stop here early when there's no data defined for the host
    return if !defined($data->{$host});

    # Only include OIDs that have data values and times;
    my @oids;
    for my $oid (keys %{$data->{$host}}) {

        my $oid_val  = $data->{$host}{$oid}{'value'};
        my $oid_time = $data->{$host}{$oid}{'time'};

        next if (!defined $oid_val || !defined $oid_time);

        push @oids, $oid;
    }

    # Get the transformed data for the val using the wanted OIDs
    my $val_data = $self->_transform_oids(\@oids, $data->{$host}, $elem_map);

    $self->logger->debug("Translated raw val data into data tree for $elem_map->{'name'}");

    # Check translated data, removing leaves and branches that were not wanted
    # !!! By design, this shouldn't be necessary as unwanted vals wont match
    # !!! This was made as a replacement for a step in the legacy code
    # !!! Running without this produces the same result
    #$val_data = $self->_trim_data($val_data->{'vals'}, $scan_data->{'vals'});
    #$self->logger->debug("Trimmed unwanted vals for $val_map->{'id'}");

    # Add the translated, cleaned data to to the val results for the host,
    # at the val_id
    $results->{'data'}{$host}{$elem_map->{'name'}} = $val_data->{'vals'};
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
sub _digest_data {

    my $self      = shift;
    my $composite = shift;   # The instance XPath from the config
    my $params    = shift;   # Parameters to request
    my $results   = shift;   # Request-global $results hash
    my $cv        = shift;   # Assumes that it's been begin()'ed with a callback

    $self->logger->debug("Digesting vals");

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
sub _do_conversions {

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

                        # Set new value to pattern match or assign as undef
                        if ($data->{$target} =~ /$temp_pattern/) {
                            unless ($exclude == 1) {
                                $new_value = $1;
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
