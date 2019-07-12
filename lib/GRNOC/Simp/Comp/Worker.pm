package GRNOC::Simp::CompData::Worker;

use strict;
### REQUIRED IMPORTS ###
use Carp;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Data::Munge qw();
use List::MoreUtils qw(any);
use Try::Tiny;
use Moo;
use AnyEvent;
use GRNOC::Log;
use GRNOC::Config;
use GRNOC::RabbitMQ::Method;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Client;
use GRNOC::WebService::Regex;


### REQUIRED ATTRIBUTES ###
=head1 public attributes

=over 12

=item config

=item logger

=item worker_id

=back

=cut

has config => ( 
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

=item client

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

has client => ( 
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

my %_FUNCTIONS; # Used by _function_one_val
my %_RPN_FUNCS; # Used by _rpn_calc


### PUBLIC METHODS ###

=head2 public_methods

=over 12

=item start

=back

=cut

sub start {

    my ( $self ) = @_;

    $self->_set_do_shutdown( 0 );

    while(1) {
        #--- we use try catch to, react to issues such as com failure
        #--- when any error condition is found, the reactor stops and we then reinitialize 
        $self->logger->debug( $self->worker_id." restarting." );
        $self->_start();
        exit(0) if $self->do_shutdown;
        sleep 2;
    }
}


sub _start {

    my ( $self ) = @_;

    my $worker_id = $self->worker_id;

    # flag that we're running
    $self->_set_is_running( 1 );

    # change our process name
    $0 = "simp_comp(worker[$worker_id])";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( "Received SIG TERM." );
        $self->_stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    my $rabbit_host = $self->config->get( '/config/rabbitmq/@ip' );
    my $rabbit_port = $self->config->get( '/config/rabbitmq/@port' );
    my $rabbit_user = $self->config->get( '/config/rabbitmq/@user' );
    my $rabbit_pass = $self->config->get( '/config/rabbitmq/@password' );
 
    $self->logger->debug( 'Setup RabbitMQ' );

    my $client = GRNOC::RabbitMQ::Client->new(  
        host        => $rabbit_host,
        port        => $rabbit_port,
        user        => $rabbit_user,
        pass        => $rabbit_pass,
        exchange    => 'Simp',
        timeout     => 15,
        topic       => 'Simp.Data' 
    );

    $self->_set_client($client);

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new( 	
        queue_name  => "Simp.CompData",
        topic       => "Simp.CompData",
        exchange    => "Simp",
        user        => $rabbit_user,
        pass        => $rabbit_pass,
        host        => $rabbit_host,
        port        => $rabbit_port 
    );

    my %predefined_param = map { $_ => 1 } ('node', 'period', 'exclude_regexp');
    
    my $composites = $self->composites;

    foreach my $composite_name (keys %$composites) {

        my $composite = $composites->{$composite_name};

        my $method = GRNOC::RabbitMQ::Method->new(
            name        => "$composite_name",
            async       => 1,
            callback    =>  sub {$self->_get($composite,@_) },
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
        my $inputs = $composite->get('/composite/variables/input');
        
        foreach my $input (@$inputs) {
            unless (exists $predefined_param{$input->{name}}) {
                $method->add_input_parameter( 
                    name        => $input->{name},
                    description => "we will add description to the config file later",
                    required    => $input->{required} ? $input->{required} : 1,
                    multiple    => 1,
                    pattern     => $GRNOC::WebService::Regex::TEXT
                );
            }
        }
        

        $dispatcher->register_method($method);
    }

    $self->config->{'force_array'} = 0;

    # Create the RabbitMQ method
    my $method2 = GRNOC::RabbitMQ::Method->new(
        name        => "ping",
        callback    =>  sub { $self->_ping() },
        description => "function to test latency"
    );

    $dispatcher->register_method( $method2 );
    $self->_set_rmq_dispatcher( $dispatcher );

    # Begin the event loop that handles requests coming in from the RabbitMQ queue
    $self->logger->debug( 'Entering RabbitMQ event loop' );
    $dispatcher->start_consuming();
    
    # If a handler calls "stop_consuming()" it removes the dispatcher definition and returns
    $self->_set_rmq_dispatcher( undef );
    return;
}


### PRIVATE METHODS ###

sub _stop {
    my $self = shift;
    $self->_set_do_shutdown( 1 );

    my $dispatcher = $self->rmq_dispatcher;
    $dispatcher->stop_consuming() if defined($dispatcher);
}


sub _ping {
    my $self = shift;
    return gettimeofday();
}


sub _get {
    my $start = [gettimeofday];
    my $self      = shift;
    my $composite = shift;
    my $rpc_ref   = shift;
    my $params    = shift;

    if ( !defined($params->{'period'}{'value'}) ) {
        $params->{'period'}{'value'} = 60;
    }

    my %results;
    ### PROCESS OVERVIEW ###
    # Processes within simp-comp work asynchronously.
    # A series of callbacks and functions use the $cv[*] AnyEvent condition variables
    # to signal the beginning and end of a process, triggering the next process when ready.
    #
    #--- Here is the process order:
    #    1. _get_scans -> _scan_cb / Processes the scan configs and then gets their data via callback
    #    2. _digest_scans         / Combines the scan data for all scans into a single OID tree
    #    3. _get_data  -> _data_cb  / Processes the val configs and then gets their data via callback
    #    4. _digest_data          / Builds data objects from all val data and then extracts them from the tree into a flat array
    #    5. _do_conversions         / Applies functions specified in configs to their val data
    #
    ### DATA STRUCTURE ###
    #
    # Data is accumulated in a global %results hash:
    #
    #--- Results from the _get_scans -> _scan_cb -> _digest_scans phase:
    # $results{scan_tree}{$node}{$scan_id} = { OID element tree returned from scans with empty leaves }
    # $results{scan_vals}{$node}{$scan_id} = { OID element tree returned from scans with hashes of their values }
    #
    # This is from legacy code, no performance is gained with it. Leaving if hidden meaning is revealed...
    # $results{scan_exclude}{$node}{$oid} = 1 (Any value here marks an oid to be excluded)
    #
    #--- Results from the _get_data -> _data_cb -> _digest_data phase:
    # $results{data}{$host}{$val_id} = { OID element tree of hashes containting data returned for the val }
    # $results{hostvar}{$host}{$hostvar_name} = The host variables (_get_data and _hostvar_cb)
    #
    #--- Results from the _do_conversions phase:
    # $results{final}{$host} = [ Array of all data objects for the host that is passed back to the caller ]
    #

    # Make sure the final results hash exists, even if we get zero results
    $results{final} = {};

    # Initialize the variables map and add any inputs to it
    $results{var_map} = {};
    my $inputs = $composite->get('/composite/variables/input');
    foreach my $input (@$inputs) {
        $results{var_map}{$input->{value}} = $input->{name};
    }

    my $success_callback = $rpc_ref->{'success_callback'};

    my @cv = map { AnyEvent->condvar; } (0..5);

    $cv[0]->begin(sub { $self->_get_scans      ($composite, $params, \%results, $cv[1]); });
    $cv[1]->begin(sub { $self->_digest_scans   ($params, \%results, $cv[2]); });
    $cv[2]->begin(sub { $self->_get_data       ($composite, $params, \%results, $cv[3]); });
    $cv[3]->begin(sub { $self->_digest_data    ($params, \%results, $cv[4]); });
    $cv[4]->begin(sub { $self->_do_conversions ($composite, $params, \%results, $cv[5]); });
    $cv[5]->begin(sub { 
        my $end = [gettimeofday];
	    my $resp_time = tv_interval($start, $end);
	    $self->logger->info("REQTIME COMP $resp_time");
		&$success_callback($results{'final'});
		undef %results;
		undef $composite;
		undef $params;
		undef @cv;
		undef $success_callback;
    });

    # Start off the pipeline:
    $cv[0]->end;
}


# Checks all branch and leaf elems of a val_tree, removing any not listed in map_tree
sub _trim_data {
    my $self     = shift;
    my $val_tree = shift;
    my $map_tree = shift;

    if (ref($val_tree) ne ref({}) ) {return;}

    # Loop over the val keys at the reference root of val
    for my $key ( keys %{$val_tree} ) {

        if ( $key eq 'value' || $key eq 'time' ) { next; }

        # Check if the key from val exists as a key in the map
        if ( exists $map_tree->{$key} ) {

            # If the existing value points to another hash
            if ( ref($val_tree->{$key}) eq ref({}) && ref($map_tree->{$key}) eq ref({}) ) {
                $self->_trim_data($val_tree->{$key}, $map_tree->{$key});
            } 
        }
        else { 
            $self->logger->debug("$key not found in map_tree, removing it from val_tree!");
            delete $val_tree->{$key}; 
        }
    }
    return $val_tree;
}


# Creates a map hash for an OID having:
# - split_oid (The OID split into it's elements)
# - base_oid  (The OID made from the max elems we can poll w/o having vars in it)
# - first_var (The index position of the first var elem)
# - last_var  (The index position of the last var elem)
# - vars      (A hash of OID elem vars set to their index in the OID)
sub _map_oid {
    my $self     = shift;
    my $oid_attr = shift;
    my %oid_map;

    $self->logger->debug("Creating a map for: $oid_attr->{name}");

    # Init the base OID for polling as the original OID
    if ($oid_attr->{oid}) {
        $oid_map{base_oid} = $oid_attr->{oid};
    }
    elsif ($oid_attr->{source}) {
        $oid_map{base_oid} = $oid_attr->{source};
    }

    # Split the oid and add that to our map
    my @split_oid = split(/\./, $oid_map{base_oid});
    $oid_map{split_oid} = \@split_oid;

    # For joining oid elements in sections
    my $last_var_idx;
    my $first_var_idx;

    # Loop over the OID elements
    for (my $i = 0; $i <= $#split_oid; $i++) {

        my $oid_elem = $split_oid[$i];

        # Change * to the name of the elem for backward compatibility
        if ( $oid_elem eq '*' ) { 
            $oid_elem      = $oid_attr->{oid_suffix};
            $split_oid[$i] = $oid_attr->{oid_suffix};
        }

        # Check if the OID element is a var or * (Regex matches on std var naming conventions)
        if ( $oid_elem =~ /^((?![\s\d])\$?[a-z]+[\da-z_-]*)*$/i ) {

            # Add the var name and it's index to the map, dependency derived from its var_num
            $oid_map{vars}{$oid_elem}{index} = $i;

            if ( !defined $first_var_idx ) {
                # Set the index where the first variable occurs
                $first_var_idx = $i;
                # Also init the last var idx
                $last_var_idx  = $i;
            }

            # Keep setting the last var to higher var positions
            if ($last_var_idx < $i) { $last_var_idx = $i; }
        }
    }

    $oid_map{first_var} = $first_var_idx;
    $oid_map{last_var}  = $last_var_idx;

    # If vars, make the base OID the elems from the first elem until we reach the first var
    if ( $first_var_idx ) {
        # The .* is appended or simp-data will match any OID suffix starting with that num
        # i.e. (1.2.3.1 would get 1.2.3.10, 1.2.3.17, 1.2.3.108, etc.)
        $oid_map{base_oid} = (join '.', @split_oid[0..($first_var_idx - 1)]) . '.*';
    }

    #$self->logger->debug("Generated map for $oid_attr->{id}:\n" . Dumper(\%oid_map));
    return \%oid_map
}


# Transforms OID values and data into a hash tree, preserving OID element dependencies
sub _transform_oids {
    my $self = shift;
    my $oids = shift;
    my $data = shift;
    my $map  = shift;
    my $type = shift;

    my %trans; # The final translation hash
    $trans{vals} = {};

    my $is_scan = 0;
    if ( defined $type && $type eq 'scan' ) {
        $trans{blanks} = {};
        $is_scan++;
    }
    else {
        $type = 'none'; 
    }
    
    my @legend; # Store vars in order of parent->child

    for my $oid ( @{$oids} ) {

        # Get the OID's returned value from polling
        my $value = $data->{$oid};
        if ( !defined $value ) { next; }

        if ( !defined $map->{first_var} ) {
            $trans{vals}{static} = $value;
            $trans{legend} = ['static'];
            if ( $is_scan ) {
                $trans{blanks}{static} = {};
            }
            next;
        }

        my @split_oid = split(/\./, $oid);

        # Check if the last var happens before the end of the data OID elements
        if ( defined $map->{last_var} && $map->{last_var} < $#split_oid ) {

            # Combine any tailing elements in the OID with the last var
            my $var_tail = join('.', splice(@split_oid, $map->{last_var}, $#split_oid));
            push(@split_oid, $var_tail);
        }

        # Make a reference point to the base of %vals
        my $ref = $trans{vals};
        my $blank_ref;

        # Do the same for blanks if it's for a scan
        if ( $is_scan ) {
            $blank_ref = $trans{blanks};
        }
        
        # Starting from the 1st var, loop over OID elements
        for ( my $i = $map->{first_var}; $i <= $#split_oid; $i++ ) {

            # Get any matching var from the map's split OID
            my $var = $map->{split_oid}[$i]; 
            if ( defined $var ) {
                if (! exists $trans{legend} ) { 
                    push @legend, $var;
                }
            }
            else {
                next;
            }

            # Get the var's value in the polled OID
            my $oid_elem = $split_oid[$i];

            # If it's not a key at the reference point, make it one and give it a val
            if (! exists $ref->{$oid_elem} ) {

                # On the last key (leaf), assign the data 
                if ( $i == $#split_oid ) {
                    $ref->{$oid_elem} = $value;
                }
                # Otherwise init a new hash in that key for another var and set it in val results
                else {
                    $ref->{$oid_elem} = {};
                }

                # Always assign blank hashes to elems in the blanks hash tree
                if ( $is_scan ) {
                    $blank_ref->{$oid_elem} = {};
                }               
            }
            # Switch the reference point to the elem's new hash
            $ref = $ref->{$oid_elem};
            if ( $is_scan ) { 
                $blank_ref = $blank_ref->{$oid_elem}; 
            };
        }

        # Set the legend if it hasn't been
        if (! exists $trans{legend} ) {
            $trans{legend} = \@legend;
        }
    }
    return \%trans;
}


# Polls for the scan elements of a composite
sub _get_scans {

    my $self      = shift;
    my $composite = shift; # The instance XPath from the config
    my $params    = shift; # Parameters to request
    my $results   = shift; # Global $results hash
    my $cv        = shift; # AnyEvent condition var (assumes it's been begin()'ed)

    $self->logger->debug("Running _get_scans");

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    # find the set of exclude patterns, and group them by var
    my %exclude_patterns;
    for my $pattern ( @{$params->{'exclude_regexp'}{'value'}} ) {
        $pattern =~ /^([^=]+)=(.*)$/;
        push @{$exclude_patterns{$1}}, $2;
    }
  
    #--- This function will execute multiple scans in "parallel" using the begin/end approach
    #--- We use $cv to signal when all those scans are done
  
    #--- these should be moved to the constructor

    # Make sure several root hashes exist
    foreach my $host (@$hosts) {
        $results->{scan_tree}{$host}    = {};
        $results->{scan_vals}{$host}    = {};
        $results->{data}{$host}         = {};
        $results->{hostvar}{$host}      = {};
    }

    my $scans = $composite->get('/composite/variables/scan');

    $self->logger->debug(Dumper($scans));

    foreach my $scan ( @$scans ) {
    # Example Scan: <scan oid_suffix="ifIdx" poll_value="ifName" oid="1.3.6.1.2.1.31.1.1.1.18.*"/>
        $results->{var_map}->{$scan->{poll_value}} = $scan->{oid_suffix};

        # Add any targets or exclusion patterns to our scan attributes
        if ( defined($scan->{var}) ) {

            if ( defined($params->{$scan->{var}}) ) {
                $scan->{targets} = $params->{$scan->{var}}{value};
            }

            if ( defined($exclude_patterns{$scan->{var}}) ) {
                $scan->{excludes} = $exclude_patterns{$scan->{var}};
            }
        } 
        else {
            $scan->{excludes} = [];
        }

        # Add a map of the scan OID to our scan attributes
        $scan->{map} = $self->_map_oid($scan);
        if ( !defined $scan->{map} ) {
            $self->logger->error("A map could not be generated for scan $scan->{oid_suffix}!");
            next;
        }

        # Get the base oid to poll, (defaults to the original OID if no vars)
        my $base_oid = $scan->{map}{base_oid};

        # Callback to get the polling data for the base oid of the scan into results
        $cv->begin;
        $self->client->get(
            node           => $hosts, 
            oidmatch       => $base_oid,
            async_callback => sub { 
                my $data = shift;
                $self->_scan_cb($data->{'results'},$hosts,$results,$scan);
                $cv->end;
            }
        );
    }

    # Signals _digest_scans to start when all callbacks complete
    $cv->end;
    $self->logger->debug("Completed _get_scans");
}


# Gets data for a scan for the OIDs we want to scan and sets scan_tree and scan_vals
sub _scan_cb {

    my $self    = shift;
    my $data    = shift;
    my $hosts   = shift;
    my $results = shift;
    my $scan    = shift;

    my $scan_map     = $scan->{map};
    my $scan_suffix  = $scan->{oid_suffix};
    my $scan_oid     = $scan->{oid};
    my $targets      = $scan->{targets};
    my $excludes     = $scan->{excludes};
    my $exclude_only = $scan->{ex_only}; # True = Add no results, but possibly blacklist OID values


    $self->logger->debug("Running _scan_cb for $scan_suffix");

    for my $host (@$hosts) {

        if ( !$data->{$host} ) {
            $self->logger->error("No scan data could be retrieved for $host");
            next;
        }
        
        # Track the OIDs we want after exclusions and targets are factored in
        my @oids;

        # Return only entries matching specified value regexps, if value regexps are specified
        my $has_targets = (defined($targets) && (scalar(@$targets) > 0));

        # Check our oids and keep only the ones we want
        for my $oid (keys %{$data->{$host}}) {

            my $oid_value = $data->{$host}{$oid}{value};

            # Blacklist the value if it matches an exclude
            if ( any { $oid_value =~ /$_/ } @$excludes ) {
                #$results->{scan_exclude}{$host}{$oid} = 1;
                # !!! The scan_exclude blacklist above is from legacy code
                # !!! Now that OIDs are only evaluated by pushing to the @oids array
                # !!! Skipping over them has the same effect
                next;
            }

            # Skip the OID if the host is exclusion only and is using target matches or the value matches a target
            if ( $exclude_only ) {
                unless ( !$has_targets || (any { $scan_oid =~ /$_/ } @$targets) ) {
                    next;
                }
            }
            # If we didn't exclude the oid, add it to our OIDs to translate
            else {
                push @oids, $oid;
            }
        }
        
        $self->logger->debug(scalar(@oids) . " OIDs were pushed for transformation");

        # Add the data for the scan to results if we're not excluding the host
        if ( !$exclude_only && scalar(@oids) ) {

            # Transform the OID data into an OID tree with empty leaves to fill
            my $scan_transform = $self->_transform_oids(\@oids, $data->{$host}, $scan_map, 'scan');

            # Set scan_tree with the blank OID tree and legend from the scan
            $results->{scan_tree}{$host}{$scan_suffix}{vals}   = $scan_transform->{blanks};
            $results->{scan_tree}{$host}{$scan_suffix}{legend} = $scan_transform->{legend};

            # Set scan_vals to the tree of values from the scan
            $results->{scan_vals}{$host}{$scan_suffix}         = $scan_transform->{vals};
            
        }
    }
    $self->logger->debug("Finished running _scan_cb for $scan_suffix");
    return;
}

# Recursively combine the OID tree for a scan with one of another scan
sub _combine_scans {
    my $self     = shift;
    my $scan     = shift;
    my $combined = shift;

    if ( ! scalar(%{$scan}) ) { return; }

    for my $key ( keys %{$scan} ) {

        if ( !exists $combined->{$key} ) {
            $combined->{$key} = {};
        }
        else {
            $self->_combine_scans($scan->{$key}, $combined->{$key});
        }
    }
}


# Process and combine the scan results once all of the scans and their callbacks have completed
sub _digest_scans {

    my $self       = shift;
    my $params     = shift; # Parameters to request
    my $results    = shift; # Request-global $results hash
    my $cv         = shift; # Assumes that it's been begin()'ed with a callback

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    $self->logger->debug("Digesting combined scans");

    for my $host ( @$hosts ) {

        my %combined_scan;

        # Get the scans for the host
        my $scans = $results->{scan_tree}{$host};
        if ( scalar(keys %{$scans}) < 1 ) {
            $self->logger->error("There is no scan data for $host!");
            next;
        }
        else {
            $self->logger->debug("_digest_scans found data for " . scalar(keys %{$scans}) . " scans");
        }

        # Single scans don't need to be combined;
        if ( scalar(keys %{$scans}) < 2 ) {
            $self->logger->debug("Single scan found, using that scan");
            for my $scan_id (keys %{$scans}) {
                $results->{scan_tree}{$host} = $results->{scan_tree}{$host}{$scan_id};
                last;
            }
            next;
        }
        # Otherwise, combine the dependent scan results for the host
        else {

            my @main_legend;
            my $main_scan;

            # Find the scan with the most dependencies and use its legend and scan val tree
            for my $scan (keys %{$scans}) {
                my $legend = $scans->{$scan}{legend};
                if ( ! @main_legend || $#main_legend < $#$legend) {
                    @main_legend = @{$legend};
                    $main_scan   = $scan;
                }
            }

            # Use our main legend and vals for the main scan as a base to combine parent scans with
            if ( @main_legend && defined $main_scan) {
                $combined_scan{legend} = \@main_legend;
                $combined_scan{vals}   = $scans->{$main_scan}{vals}
            }
            else {
                $self->logger->error("No legend was found for any scans");
                return;
            }

            # Loop over the parent scans, combining them into one OID tree
            for ( my $i = 0; $i < $#main_legend; $i++ ) {
                my $scan = $scans->{$main_legend[$i]}{vals};
                $self->_combine_scans($scan, $combined_scan{vals});
            }
        }
        # Replace our scanned OID trees for the host with one combined one
        $results->{scan_tree}{$host} = \%combined_scan;
    }
    $self->logger->debug("Finished digesting scans");
    $cv->end;
}


# Fetches the host variables and SNMP values for <val> elements
sub _get_data {

    my $self      = shift;
    my $composite = shift; # The instance XPath from the config
    my $params    = shift; # Parameters to request
    my $results   = shift; # Global $results hash
    my $cv        = shift; # AnyEvent condition var (assumes it's been begin()'ed)

    $self->logger->debug("Running _get_data");

    # Get the set of required variables
    my $hosts = $params->{'node'}{'value'};
    
    # This callback does multiple gets in "parallel" using the begin/end apprach
    # $cv is used to signal when the gets are done
    $cv->begin;
    $self->client->get(
        node           => $hosts,
        oidmatch       => 'vars.*',
        async_callback => sub {
            my $data = shift;
            $self->_hostvar_cb($data->{'results'}, $results);
            $cv->end;
        },
    );

    # Initialize data elements 
    my @data_elements;
    
    my $value_data = $composite->get('/composite/data/value');
    my $meta_data  = $composite->get('/composite/data/meta');

    # Create a name map for metadata elements to handle the TSDS asterisk notation
    foreach my $meta_elem (@$meta_data) {
        # Map the original name to the adjusted one for functions
        $results->{meta}->{$meta_elem->{name}} = "*$meta_elem->{name}";
    }

    push(@data_elements, @$value_data);
    push(@data_elements, @$meta_data);
    $self->logger->debug(Dumper(\@data_elements));

    foreach my $elem (@data_elements) {

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


        # Check if the element source isn't an OID
        unless ( $elem->{source} =~ /.*\.+.*/ ) {

            # If the val is the "node" var, create a val object in results with one value set to the host
            if ( $elem->{name} eq 'node' ) {
                foreach my $host (@$hosts) {
                    $results->{data}{$host}{node}{value} = $host;
                }
                next;
            }
            else {
                # Add the scan_vals hash for it to the results val hash under its val ID
                my $val_key = $results->{var_map}->{$elem->{source}};
                $self->logger->debug("Getting data for source $elem->{source} from the $val_key values");
                foreach my $host (@$hosts) {
                    if ( exists $results->{scan_vals}{$host}{$val_key} ) {
                        $results->{data}{$host}{$elem->{name}} = $results->{scan_vals}{$host}{$val_key};
                    }
                }
                next;
            }
        }
        # Pull the element's OID data from Simp
        else {

            # Create a map of the val OID for use
            $elem->{map} = $self->_map_oid($elem);
            if ( !defined $elem->{map} ) {
                $self->logger->error("A map could not be generated for val $elem->{name}!");
                next;
            };

            # Add the element's name to the val_map
            $elem->{map}{name} = $elem->{name};

            # Get the base OID of the val for polling 
            my $base_oid = $elem->{map}{base_oid};

            foreach my $host (@$hosts) {

                if ( scalar(keys %{$results->{scan_tree}{$host}}) < 1 ) {
                    $self->logger->error("ERROR: No scan data! Skipping vals for $host");
                    next;
                }

                # Get the data for these OIDs from Simp
                $cv->begin;

                # It is tempting to request just the OIDs you know you want,
                # instead of asking for the whole subtree, but requesting
                # a bunch of individual OIDs takes SimpData a *whole* lot
                # more time and CPU, so we go for the subtree.
                if ( defined($elem->{type}) && $elem->{type} eq 'rate' ) {

                    $self->client->get_rate(
                        node     => [$host],
                        period   => $params->{'period'}{'value'},
                        oidmatch => [$base_oid],
                        async_callback => sub {
                            my $data = shift;
                            $self->_data_cb($data->{'results'},$results,$host,$elem->{map});
                            $cv->end;
                        }
                    );

                } 
                else {
                    $self->client->get(
                        node     => [$host],
                        oidmatch => [$base_oid],
                        async_callback => sub {
                            my $data = shift;
                            $self->_data_cb($data->{'results'},$results,$host,$elem->{map});
                            $cv->end;
                        }
                    );
                }
            }
        }
    }
    $cv->end;
    $self->logger->debug("Finished running _get_data");
}

# Not sure what the point of this is, perhaps to queue data?
sub _hostvar_cb {

    my $self    = shift;
    my $data    = shift;
    my $results = shift;

    $self->logger->debug("Running _hostvar_cb");

    foreach my $host (keys %$data) {
        foreach my $oid (keys %{$data->{$host}}) {
            my $val = $data->{$host}{$oid}{'value'};
            $oid =~ s/^vars\.//;
            $results->{'hostvar'}{$host}{$oid} = $val;
        }
    }

    $self->logger->debug("Finished running _hostvar_cb");
}


# Callback to get data for a val
sub _data_cb {

    my $self     = shift;
    my $data     = shift;
    my $results  = shift;
    my $host     = shift;
    my $elem_map = shift;

    # Get the scan data for the host
    my $scan_data = $results->{scan_tree}{$host};
    $self->logger->debug("Scan data found for _data_cb: " . scalar(keys %{$scan_data}));

    $self->logger->debug("Running _data_cb");

    # Stop here early when there's no data defined for the host
    return if !defined($data->{$host});
    
    # Only include OIDs that have data values and times;
    my @oids;
    for my $oid ( keys %{$data->{$host}} ) {
        my $oid_val  = $data->{$host}{$oid}{value};
        my $oid_time = $data->{$host}{$oid}{time};

        if ( !defined $oid_val || !defined $oid_time ) { next; }

        push @oids, $oid;
    };

    # Get the transformed data for the val using the wanted OIDs
    my $val_data = $self->_transform_oids(\@oids, $data->{$host}, $elem_map);
    $self->logger->debug("Translated raw val data into data tree for $elem_map->{name}");

    # Check translated data, removing leaves and branches that were not wanted
    # !!! By design, this shouldn't be necessary as unwanted vals wont match
    # !!! This was made as a replacement for a step in the legacy code
    # !!! Running without this produces the same result
    #$val_data = $self->_trim_data($val_data->{vals}, $scan_data->{vals});
    #$self->logger->debug("Trimmed unwanted vals for $val_map->{id}");

    # Add the translated, cleaned data to to the val results for the host, at the val_id
    $results->{data}{$host}{$elem_map->{name}} = $val_data->{vals};

    return;
}


# Adds all value leaves of a val_tree to the data_tree's hash leaves
sub _build_data {
    my $self      = shift;
    my $val_id    = shift;
    my $val_tree  = shift;
    my $data_tree = shift;


    # Check if our data tree reference is a leaf on the tree
    #if ( ref($data_tree) eq ref({}) && (!keys $data_tree || exists $data_tree->{time}) ) {
    if ( exists $data_tree->{time} || !scalar(%{$data_tree}) ) {

        # Ensure that we have a value to add to the leaf
        if ( exists $val_tree->{value} ) {

            # Set the value for the val
            $data_tree->{$val_id} = $val_tree->{value};

            # Set time once per leaf
            if ( !exists $data_tree->{time} ) {
                if ( exists $val_tree->{time} ) {
                    $data_tree->{time} = $val_tree->{time};
                }
                else {
                    $data_tree->{time} = time;
                }
            }
        }
    }
    else{
        # Loop over the all the relevant keys of the data tree
        for my $key ( keys %{$data_tree} ) {

            # The data values haven't been reached yet
            if ( !exists $val_tree->{value} ) {

                # Check that the val_tree follows the path along the data tree
                if ( exists $val_tree->{$key} ) {

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
        if (exists $data_tree->{$key}{time}) {
            push @{$output}, $data_tree->{$key};
        }
        else {
            $self->_extract_data($data_tree->{$key}, $output);
        }
    }
}


# Digests the val data and transforms it into an array of data objects after all callbacks complete
sub _digest_data {

    my $self       = shift;
    my $params     = shift; # Parameters to request
    my $results    = shift; # Request-global $results hash
    my $cv         = shift; # Assumes that it's been begin()'ed with a callback

    $self->logger->debug("Digesting vals");

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    for my $host ( @$hosts ) {

        if (! keys %{$results->{scan_tree}{$host}} ) {
            $self->logger->error("No vals were returned for $host");
            $results->{data}{$host} = [];
            next;
        }

        # Reuse the scan tree for the host to build the data
        my $val_tree = $results->{scan_tree}{$host}{vals};

        # Get the vals polled for the host
        my $vals = $results->{data}{$host};

        # Add the data for all vals to the appropriate leaves of val_tree
        for my $val_id ( keys %{$vals} ) {
            $self->logger->debug("Building data for $val_id");
            $self->_build_data($val_id, $vals->{$val_id}, $val_tree);
        }
        $self->logger->debug("Finished building val data");

        # Construct the final, flattened data array from the completed val_tree
        my @val_data;
        $self->_extract_data($val_tree, \@val_data);
        $self->logger->debug("Extracted val data objects for $host");
        # Set the results for val for the host to the data
        $results->{data}{$host} = \@val_data;
    }
    $self->logger->debug("Finished digesting vals");
    $cv->end;
}


# Applies conversion functions to values gathered by _get_data
sub _do_conversions {

    my $self      = shift;
    my $composite = shift; # The instance XPath from the config
    my $params    = shift; # Parameters to request
    my $results   = shift; # Global $results hash
    my $cv        = shift; # AnyEvent condition var (assumes it's been begin()'ed)

    # Get array of all conversions in-order
    my $conversions = $composite->get('/composite/conversions/*');
    foreach my $c (@$conversions) {
        if (exists $c->{definition}) {
            $c->{type} = 'function';
        } 
        elsif (exists $c->{this}) {
            $c->{type} = 'replace';
        } 
        elsif (exists $c->{pattern}) {
            $c->{type} = 'match';
        }
    }
    $self->logger->debug(Dumper($conversions));

    $self->logger->debug("Applying conversions to the data");

    my $now = time;

    # Iterate over the data array for each host
    foreach my $host (keys %{$results->{data}}) {

        # Initialise the final data array for the host
        $results->{final}{$host} = [];

        # Skip and use empty results if host has no data
        if ( !$results->{data}{$host} || (ref($results->{data}{$host}) ne ref([])) ) {
            $self->logger->error("ERROR: No data array was generated for $host!");
            next;
        }

        # Set final values if no functions are being applied to the hosts's data 
        unless ( $conversions ) {
            $results->{final}{$host} = $results->{data}{$host};
            next; 
        }

        # Apply each function included in conversions
        foreach my $conversion (@$conversions) {

            # Get the target data for the conversion
            my $targets = $conversion->{data};

            # Apply the conversion to each data target
            foreach my $target (keys %$targets) {

                # Make a hash of all var names mentioned in the conversion (avoids dupes)
                my %vars;

                # Initialize all conversion attributes specific to the target
                my $target_def     = $conversion->{definition};
                my $target_with    = $conversion->{with};
                my $target_this    = $conversion->{this};
                my $target_pattern = $conversion->{pattern};

                # Functions
                if ($conversion->{type} eq 'function') {
                    
                    $target_def =~ s/\$\{\}/\$\{$target\}/g;

                    %vars = map {$_ => 1} $target_def =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;

                    $self->logger->debug("Function has the following definition and vars:");
                    $self->logger->debug(Dumper($target_def));
                    $self->logger->debug(Dumper(\%vars));
                }
                # Replacements
                elsif ($conversion->{type} eq 'replace') {

                    $target_with =~ s/\$\{\}/\$\{$target\}/g;
                    $target_this =~ s/\$\{\}/\$\{$target\}/g;

                    # Combine strings so vars can be found easier in both
                    my $combo = $target_with . $target_this;

                    %vars = map {$_ => 1} $combo =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;
                    
                    $self->logger->debug("Replacement has the follwing vars:");
                    $self->logger->debug(Dumper(\%vars));
                }
                # Matches
                elsif ($conversion->{type} eq 'match') {

                    $target_pattern =~ s/\$\{\}/\$\{$target\}/g;
                    
                    %vars = map {$_ => 1} $target_pattern =~ m/\$\{([a-zA-Z0-9_-]+)\}/g;
                    
                    $self->logger->debug("Match has the following vars:");
                    $self->logger->debug(Dumper(\%vars));
                }

                # Apply the conversion for the target to each data object for the host
                foreach my $data ( @{$results->{data}{$host}} ) {

                    # Initialize all temporary conversion attributes for the data object
                    my $temp_def     = $target_def;
                    my $temp_this    = $target_this;
                    my $temp_with    = $target_with;
                    my $temp_pattern = $target_pattern;

                    # Track any missing variable values or errors
                    my $data_errors = 0;

                    # Ensure the data object has a time stamp
                    unless (exists $data->{time}) {
                        $data->{time} = $now;
                    }

                    # Skip if the target data element doesn't exist in the object
                    unless (exists $data->{$target}) {
                        #$self->logger->error("ERROR: The target data \"$target\" doesn't exist for $host!");
                        next;
                    }

                    # Replace data variables in the definition with their value
                    foreach my $var (keys %vars) {
                        $self->logger->debug("VARIABLE: $var");

                        # Add error and skip the var if there's no data for it
                        unless (exists $data->{$var}) {
                            $data_errors++;
                            next;
                        }
                        $self->logger->debug("Checking value for $var: \"$data->{$var}\"");

                        # Get the data's value for the var
                        my $var_value = $data->{$var};

                        # Functions
                        if ($conversion->{type} eq 'function') {

                            # Replace the var indicators with their value
                            $temp_def =~ s/\$\{$var\}/$var_value/;
                            $self->logger->debug("Applying function: $temp_def");
                        }
                        # Replacements
                        elsif ($conversion->{type} eq 'replace') {

                            # Replace the var indicators with their value
                            $temp_with =~ s/\$\{$var\}/$var_value/g;
                            $temp_this =~ s/\$\{$var\}/$var_value/g;
                            $self->logger->debug("Replacing \"$temp_this\" with \"$temp_with\"");
                        }
                        # Matches
                        elsif ($conversion->{type} eq 'match') {

                            # Replace the var indicators with their value
                            $temp_pattern =~ s/\$\{$var\}/\Q$var_value\E/;
                            $self->logger->debug($temp_pattern);
                        }
                    }

                    # If the function can't be completed, don't send a value for the data
                    if ($data_errors) {
                        #$self->logger->error("ERROR: There were $data_errors missing data points!");
                        $data->{$target} = undef;
                        next;
                    }
                
                    # Store the converted value
                    my $new_value;
 
                    # Function application
                    if ($conversion->{type} eq 'function') {

                        # Use the FUNCTIONS object rpn definition
                        $new_value = $_FUNCTIONS{rpn}([$data->{$target}],$temp_def,$conversion,$data,$results,$host)->[0];
                    }
                    # Replacement application
                    elsif ($conversion->{type} eq 'replace') {

                        # Use the replace module to replace the value
                        $new_value =  Data::Munge::replace($data->{$target}, $temp_this, $temp_with);
                    }
                    # Match application
                    elsif ($conversion->{type} eq 'match') {

                        # Set new value to pattern match or assign as undef
                        if ($data->{$target} =~ /$temp_pattern/){
                            $new_value = $1;
                        }
                    }
                    
                    # Assign the new value to the data target
                    $data->{$target} = $new_value;

                    # Prepend an asterisk to metadata names for TSDS
                    #foreach my $meta_name (keys %{$results->{meta}}) {
                    #    if (exists $data->{$meta_name}) {
                            # Set the new key to its value and delete the old key pair
                    #        $data->{$results->{meta}{$meta_name}} = delete $data->{$meta_name};
                    #    }
                    #}
                }
            }
        }
        # Once any/all functions are applied in results->val for the host, we set the final data to that hash
        $results->{final}{$host} = $results->{data}{$host};
    }
    $self->logger->debug(Dumper($results->{final}));
    $self->logger->debug("Finished applying conversions to the data");
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
	foreach my $val (@$vals){
	    $new_val += $val;
	}
	return [$new_val];
    },
    'max' => sub {
	my ($vals, $operand) = @_;
        my $new_val;
        foreach my $val (@$vals){
	    if(!defined($new_val)){
		$new_val = $val;
	    }else{
		if($val > $new_val){
		    $new_val = $val;
		}
	    }
        }
        return [$new_val];
    },
    'min' => sub {
        my ($vals, $operand) = @_;
        my $new_val;
        foreach my $val (@$vals){
	    if(!defined($new_val)){
                $new_val = $val;
            }else{
                if($val < $new_val){
                    $new_val = $val;
                }
            }
        }
        return [$new_val];
    },
    '+' => sub { # addition
        my ($vals, $operand) = @_;
	foreach my $val (@$vals){
	    return [$val] if !defined($val);
	    return [$val + $operand];
	}
    },
    '-' => sub { # subtraction
        my ($vals, $operand) = @_;
	foreach my $val( @$vals){
	    return [$val] if !defined($val);
	    return [$val - $operand];
	}
    },
    '*' => sub { # multiplication
        my ($vals, $operand) = @_;
	foreach my $val (@$vals){
	    return [$val] if !defined($val);
	    return [$val * $operand];
	}
    },
    '/' => sub { # division
        my ($vals, $operand) = @_;
	foreach my $val (@$vals){
	    return [$val] if !defined($val);
	    return [$val / $operand];
	}
    },
    '%' => sub { # modulus
        my ($vals, $operand) = @_;
	foreach my $val (@$vals){
	    return [$val] if !defined($val);
	    return [$val % $operand];
	}
    },
    'ln' => sub { # base-e logarithm
        my $vals = shift;
	foreach my $val (@$vals){
	    return [$val] if !defined($val);
	    return [] if $val == 0;
	    eval { $val = log($val); }; # if val==0, we want the result to be undef, so this works just fine
	    return [$val];
	}
    },
    'log10' => sub { # base-10 logarithm
        my $vals = shift;
	foreach my $val (@$vals){
	    return [$val] if !defined($val);
	    $val = eval { log($val); }; # see ln
	    $val /= log(10) if defined($val);
	    return [$val];
	}
    },
    'match' => sub { # regular-expression match and extract first group
        my ($vals, $operand) = @_;
	foreach my $val (@$vals){
	    if($val =~ /$operand/){
		return [$1];
	    }
	    return [$val];
	}
    },
    'replace' => sub { # regular-expression replace
        my ($vals, $operand, $replace_with) = @_;
	foreach my $val (@$vals){
	    $val = Data::Munge::replace($val, $operand, $replace_with);
	    return [$val];
	}
    },
    'rpn' => sub { return [ _rpn_calc(@_) ]; },
);

sub _rpn_calc{
    my ($vals, $operand, $fctn_elem, $val_set, $results, $host) = @_;

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
	
	    my %func_lookup_errors;
	    my @prog_copy = @prog;
	
	    # Now, go through the program, one token at a time:
	    foreach my $token (@prog) {
	        # Handle some special cases of tokens:
	        if($token =~ /^[\'\"]/){ # quoted strings
		        # Take off the start and end quotes, including
		        # the handling of unterminated strings:
		        if($token =~ /^\"/) {
		            $token =~ s/^\"(([^\"\\]|\\.)*)[\"\\]?$/$1/;
		        } else {
		            $token =~ s/^\'(([^\'\\]|\\.)*)[\'\\]?$/$1/;
		        }
		        $token =~ s/\\(.)/$1/g; # unescape escapes
		        push @stack, $token;
	        }
            elsif ($token =~ /^[+-]?([0-9]+\.?|[0-9]*\.[0-9]+)$/) { # decimal numbers
		        push @stack, ($token + 0);
	        }
            elsif ($token =~ /^\$/) { # name of a value associated with the current (host, OID suffix)
		        push @stack, $val_set->{substr $token, 1};
	        }
            elsif ($token =~ /^\#/) { # host variable
		        push @stack, $results->{'hostvar'}{$host}{substr $token, 1};
	        }
            elsif ($token eq '@') { # push hostname
		        push @stack, $host;
	        }
            else { # treat as a function
		        if (!defined($_RPN_FUNCS{$token})) {
		            GRNOC::Log::log_error("RPN function $token not defined!") if !$func_lookup_errors{$token};
		            $func_lookup_errors{$token} = 1;
		            next;
                }
		        $_RPN_FUNCS{$token}(\@stack);
	        }
	    
	        # We copy, as in certain cases Dumper() can affect the elements of values passed to it
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
        my $b = pop @$stack;
        my $a = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? $a+$b : undef;
    },
    # minuend subtrahend => difference
    '-' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? $a-$b : undef;
    },
    # multiplicand1 multiplicand2 => product
    '*' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        push @$stack, (defined($a) && defined($b)) ? $a*$b : undef;
    },
    # dividend divisor => quotient
    '/' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $x = eval { $a / $b; }; # make divide by zero yield undef
        push @$stack, (defined($a) && defined($b)) ? $x : undef;
    },
    # dividend divisor => remainder
    '%' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $x = eval { $a % $b; }; # make divide by zero yield undef
        push @$stack, (defined($a) && defined($b)) ? $x : undef;
    },
    # number => logarithm_base_e
    'ln' => sub {
        my $stack = shift;
        my $x = pop @$stack;
        $x = eval { log($x); }; # make ln(0) yield undef
        push @$stack, $x;
    },
    # number => logarithm_base_10
    'log10' => sub {
        my $stack = shift;
        my $x = pop @$stack;
        $x = eval { log($x); }; # make ln(0) yield undef
        $x /= log(10) if defined($x);
        push @$stack, $x;
    },
    # number => power
    'exp' => sub {
        my $stack = shift;
        my $x = pop @$stack;
        $x = eval { exp($x); } if defined($x);
        push @$stack, $x;
    },
    # base exponent => power
    'pow' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $x = eval { $a ** $b; };
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
        my $a = pop @$stack;
        push @$stack, _bool_to_int(defined($a));
    },

    # a b => (is a numerically equal to b? (or both undef))
    '==' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (defined($a) && defined($b)) ? ($a == $b) :
                  (!defined($a) && !defined($b)) ? 1 :
                  0;
        push @$stack, _bool_to_int($res);
    },
    # a b => (is a numerically unequal to b?)
    '!=' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (defined($a) && defined($b)) ? ($a != $b) :
                  (!defined($a) && !defined($b)) ? 0 :
                  1;
        push @$stack, _bool_to_int($res);
    },
    # a b => (is a numerically less than b?)
    '<' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (defined($a) && defined($b)) ? ($a < $b) : 0;
        push @$stack, _bool_to_int($res);
    },
    # a b => (is a numerically less than or equal to b?)
    '<=' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (defined($a) && defined($b)) ? ($a <= $b) : 0;
        push @$stack, _bool_to_int($res);
    },
    # a b => (is a numerically greater than b?)
    '>' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (defined($a) && defined($b)) ? ($a > $b) : 0;
        push @$stack, _bool_to_int($res);
    },
    # a b => (is a numerically greater than or equal to b?)
    '>=' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (defined($a) && defined($b)) ? ($a >= $b) : 0;
        push @$stack, _bool_to_int($res);
    },

    # a b => (a AND b)
    'and' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        push @$stack, _bool_to_int($a && $b);
    },
    # a b => (a OR b)
    'or' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        push @$stack, _bool_to_int($a || $b);
    },
    # a => (NOT a)
    'not' => sub {
        my $stack = shift;
        my $a = pop @$stack;
        push @$stack, _bool_to_int(!$a);
    },

    # pred a b => (a if pred is true, b if pred is false)
    'ifelse' => sub {
        my $stack = shift;
        my $b    = pop @$stack;
        my $a    = pop @$stack;
        my $pred = pop @$stack;
        push @$stack, (($pred) ? $a : $b);
    },

    # string pattern => match_group_1
    'match' => sub {
        my $stack = shift;
        my $pattern = pop @$stack;
        my $string = pop @$stack;
        if($string =~ /$pattern/){
            push @$stack, $1;
        }else{
            push @$stack, undef;
        }
    },
    # string match_pattern replacement_pattern => transformed_string
    'replace' => sub {
        my $stack = shift;
        my $replacement = pop @$stack;
        my $pattern     = pop @$stack;
        my $string      = pop @$stack;

        if(!defined($string) || !defined($pattern) || !defined($replacement)){
            push @$stack, undef;
            return;
        }

        $string = Data::Munge::replace($string, $pattern, $replacement);
        push @$stack, $string;
    },
    # string1 string2 => string1string2
    'concat' => sub {
        my $stack = shift;
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
        my $a = pop @$stack;
        if(!defined($a) || ($a+0) < 1){
            push @$stack, undef;
            return;
        }
        push @$stack, $stack->[-($a+0)]; # This pushes undef if $a is greater than the stack size, which is OK
    },
);


1;
