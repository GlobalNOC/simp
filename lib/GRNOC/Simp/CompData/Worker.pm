package GRNOC::Simp::CompData::Worker;

use strict;
### REQUIRED IMPORTS ###
use Carp;
use Clone qw(clone);
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Data::Munge qw();
use List::MoreUtils qw(any);
use Try::Tiny;
use Moo;
use AnyEvent;
use GRNOC::Log;
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
    is          => 'ro',
    required    => 1 
);

has logger => ( 
    is          => 'ro',
    required    => 1 
);

has worker_id => ( 
    is          => 'ro',
    required    => 1 
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
    $0 = "comp_data ($worker_id) [worker]";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info( "Received SIG TERM." );
        $self->_stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    my $rabbit_host = $self->config->get( '/config/rabbitMQ/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbitMQ/@port' );
    my $rabbit_user = $self->config->get( '/config/rabbitMQ/@user' );
    my $rabbit_pass = $self->config->get( '/config/rabbitMQ/@password' );
 
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

    #--- parse config and create methods based on the set of composite definitions.
    $self->config->{'force_array'} = 1;
    my $allowed_methods = $self->config->get( '/config/composite' );
    $self->logger->debug(Dumper($allowed_methods));

    my %predefined_param = map { $_ => 1 } ('node', 'period', 'exclude_regexp');

    foreach my $meth (@$allowed_methods) {
        my $method_id = $meth->{'id'};
        print "$method_id:\n";

        my $method = GRNOC::RabbitMQ::Method->new(
            name        => "$method_id",
            async       => 1,
            callback    =>  sub {$self->_get($method_id,@_) },
            description => "retrieve composite simp data of type $method_id, we should add a descr to the config" 
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

        #--- let xpath do the iteration for us
        my $path = "/config/composite[\@id=\"$method_id\"]/input";
        my $inputs = $self->config->get($path);

        foreach my $input (@$inputs) {

            my $input_id = $input->{'id'};
            next if $predefined_param{$input_id};

            my $required = 0;
            if ( defined $input->{'required'} ) { $required = 1; }

            $method->add_input_parameter( 
                name => $input_id,
                description => "we will add description to the config file later",
                required => $required,
                multiple => 1,
                pattern => $GRNOC::WebService::Regex::TEXT
            );
        
            print "  $input_id: $required:\n";
        }

        $dispatcher->register_method($method);
    }

    $self->config->{'force_array'} = 0;

    #--------------------------------------------------------------------------

    my $method2 = GRNOC::RabbitMQ::Method->new(
        name => "ping",
        callback =>  sub { $self->_ping() },
        description => "function to test latency"
    );

    $dispatcher->register_method( $method2 );
    $self->_set_rmq_dispatcher( $dispatcher );

    #--- go into event loop handing requests that come in over rabbit  
    $self->logger->debug( 'Entering RabbitMQ event loop' );
    $dispatcher->start_consuming();
    
    #--- you end up here if one of the handlers called stop_consuming
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

    #--- figure out hostType
    my $hostType = "default";

    #--- give up on config object and go direct to xmllib to get proper xpath support
    my $doc = $self->config->{'doc'};
    my $xpc = XML::LibXML::XPathContext->new($doc);

    #--- get the instance
    my $path = "/config/composite[\@id=\"$composite\"]/instance[\@hostType=\"$hostType\"]";
    my $ref = $xpc->find($path);

    ### PROCESS OVERVIEW ###
    #--- We have to do things asynchronously, so execution from here follows
    #--- a series of callbacks, tied together using the $cv[*] condition variables:
    #--- _do_scans       -> _do_vals       -> _do_functions -> success
    #---     \->_scan_cb      | \->_val_cb
    #---                      \->_hostvar_cb
    #
    # Data is accumulated in the %results hash, which has the following structure:
    #
    # $results{'scan'}{$node}{$var_name} = [ list of OID suffixes ]
    #    * The results from the scan phase (_do_scans and _scan_cb)
    # $results{'scan-exclude'}{$node}{$oid_suffix} = 1
    #    * If present, exclude the OID suffix from results for that node
    # $results{'scan-match'}{$node}{$var_name}{$oid_suffix} = $val
    #    * Mapping from (scan-variable name, OID suffix) to value at OID
    # $results{'val'}{$host}{$oid_suffix}{$var_name} = $val
    #    * The results from the get-values phase (_do_vals and _val_cb)
    # $results{'hostvar'}{$host}{$hostvar_name} = $val
    #    * The host variables (_do_vals and _hostvar_cb)
    # $results{'final'}{$host}{$oid_suffix}{$var_name} = $val
    #    * The results from the compute-functions phase (_do_functions);
    #      $results{'final'} is passed back to the caller

    # Make sure this exists, even if we get zero results
    $results{'final'} = {};

    my $success_callback = $rpc_ref->{'success_callback'};

    my @cv = map { AnyEvent->condvar; } (0..5);

    $cv[0]->begin(sub { $self->_do_scans($ref, $params, \%results, $cv[1]); });
    $cv[1]->begin(sub { $self->_digest_scans($ref, $params, \%results, $cv[2]); });
    $cv[2]->begin(sub { $self->_do_vals($ref, $params, \%results, $cv[3]); });
    $cv[3]->begin(sub { $self->_digest_vals($ref, $params, \%results, $cv[4]); });
    #$cv[4]->begin(sub { $self->_do_functions($ref, $params, \%results, $cv[5]); });
    $cv[5]->begin(sub { 
        my $end = [gettimeofday];
	    my $resp_time = tv_interval($start, $end);
	    $self->logger->info("REQTIME COMP $resp_time");
		&$success_callback($results{'final'});
		undef %results;
		undef $ref;
		undef $params;
		undef @cv;
		undef $success_callback;
    });

    # Start off the pipeline:
    $cv[0]->end;
}


# Helper function for _map_oid that will nest oid vars within their dependent vars (vars to the left)
sub _nest_oid_vars {
    my $self     = shift;
    my $var_data = shift;

    # Ensure input was OID var array
    if ( ref($var_data) eq ref([]) ) {

        # The final hash of nested vars and vals to return
        my %var_nest; 

        # Loop through the data
        for (my $i = $#$var_data; $i >= 0; $i--) {

            # Get our var name and it's index from the data
            my $var = $var_data->[$i][0];
            my $idx = $var_data->[$i][1];

            # Set first assignment if it's the rightmost OID var
            if (! %var_nest ) {
                %var_nest = ( 
                    name => $var,
                    idx  => $idx
                );
            }

            # Otherwise, set the current var to hold its value and the last var etc
            else {
                # Build our new hash in a temporary hash
                my %temp = (
                    name => $var,
                    idx  => $idx,
                    var  => {%var_nest}
                );

                # Swap the hashes and end the loop
                %var_nest = %temp;
            }
            $self->logger->debug(Dumper(\%var_nest));
        }
        $self->logger->debug("Final Hash:\n" . Dumper(\%var_nest));

        # Return reference to the final hash
        return \%var_nest;

    } else {
        $self->logger->error("_nest_oid_vars was not given an array of [var, val] combinations!");
        return;
    }
}


# Checks all branches and leaves of val_tree, removing any not listed in map_tree
sub _trim_tree {
    my $self     = shift;
    my $val_tree = shift;
    my $map_tree = shift;

    if (ref($val_tree) ne ref({}) ) {return;}

    # Loop over the val keys at the reference root of val
    for my $key ( keys %{$val_tree} ) {

        if ( $key eq 'value' || $key eq 'time' ) { next; }

        $self->logger->debug("_trim_trees checking key: $key");

        # Check if the key from val exists as a key in the map
        if ( exists $map_tree->{$key} ) {

            # If the existing value points to another hash
            if ( ref($val_tree->{$key}) eq ref({}) && ref($map_tree->{$key}) eq ref({}) ) {
                $self->logger->debug("Recursing through next level of val & map trees for key: $key");
                $self->_trim_tree($val_tree->{$key}, $map_tree->{$key});
            } 
        }
        else { 
            $self->logger->debug("$key not found in map_tree, removing it from val_tree!");
            delete $val_tree->{$key}; 
        }
    }
    return $val_tree;
}


# Creates a hash mapping for the OID, split OID, vars and their position, and the OID trunk index
sub _map_oid {
    my $self    = shift;
    my $oid     = shift;
    my %oid_map;

    $self->logger->debug("Creating a map for OID: $oid");

    # Split the oid and add that to our map
    my @split_oid = split(/\./, $oid);
    $oid_map{split_oid} = \@split_oid;

    # Loop over the OID elements
    for (my $i = 0; $i <= $#split_oid; $i++) {

        my $oid_elem = $split_oid[$i];

        # Check if the OID element is a var (Regex matches on std var naming conventions)
        if ($oid_elem =~ /^((?![\s\d])[a-z]+[\da-z_-]*)*$/i) {

            # Add the var name and it's index to the map, dependency derived from its var_num
            $oid_map{vars}{$oid_elem}{index} = $i;

            if (! exists $oid_map{trunk} ) {
            # Set the oid trunk where the first variable occurs
                if ($i > 0) {
                    $oid_map{trunk} = $i - 1;
                } 
                else {
                    $oid_map{trunk} = $i;
                }
            }
        }
    }
    return \%oid_map
}


# Transforms OID values and data into a tree with preserved dependencies
sub _translate_oids {
    my $self     = shift;
    my $oids     = shift;
    my $data     = shift;
    my $map      = shift;
    my $type     = shift;

    if (! defined $type ) { $type = 'default'; }
 
    my %trans;   # Final translated hash
    my %vals;    # Temporary hash for building values
    my @legend;  # Store vars in order of parent->child

    for my $oid ( @{$oids} ) {

        # Get the OID's returned value from polling
        my $value = $data->{$oid};

        my @split_oid = split(/\./, $oid);

        # Make a reference point starting at the base of values in %trans
        my $ref = \%vals;

        # Starting from the 1st var at the mapped trunk, loop over OID elements
        for ( my $i = $map->{trunk} + 1; $i <= $#split_oid; $i++ ) {

            # Get any matching var from the map's split OID
            my $var = $map->{split_oid}[$i];
            if (! exists $map->{vars}{$var} ) { next; }

            # Get the var's value in the polled OID
            my $val = $split_oid[$i];

            # If it's not a key at the reference point, make it and give it a val
            if (! exists $ref->{$val} ) {
                # On the last key, our value will be the OID's polled result
                if ( $i == $#split_oid && $type ne 'scan') {
                    $ref->{$val} = $value;
                }
                # Otherwise init a new hash in that key for another var
                else {
                    $ref->{$val} = {};
                }
            }

            # Switch the reference point to the new key's hash
            $ref = $vals{$val};

            # Push vars to the legend if it hasn't been set
            if (! exists $trans{legend} ) {
                push @legend, $var;
            }
        }
        
        # Set the legend if it hasn't been
        if (! exists $trans{legend} ) {
            $trans{legend} = \@legend;
        }

    }

    # Add the vals to %trans
    $trans{vals} = \%vals;

    $self->logger->debug(Dumper(\%trans));
    return \%trans;
}


sub _do_scans {

    my $self       = shift;
    my $composites = shift; # top-level XML element for CompData instance
    my $params     = shift; # parameters to request
    my $results    = shift; # request-global $results hash
    my $cv         = shift; # assumes that it's been begin()'ed with a callback

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    # find the set of exclude patterns, and group them by var
    my %exclude_patterns;
    foreach my $pattern (@{$params->{'exclude_regexp'}{'value'}}) {
        $pattern =~ /^([^=]+)=(.*)$/;
        push @{$exclude_patterns{$1}}, $2;
    }
  
    #--- this function will execute multiple scans in "parallel" using the begin / end approach
    #--- we use $cv to signal when all those scans are done
  
    #--- give up on config object and go direct to xmllib to get proper xpath support
    #--- these should be moved to the constructor
    my $doc = $self->config->{'doc'};
    my $xpc = XML::LibXML::XPathContext->new($doc);

    # Make sure several root hashes exist
    foreach my $host (@$hosts) {
        $results->{'scan'}{$host} = {};
        $results->{'scan-exclude'}{$host} = {};
        $results->{'scan-match'}{$host} = {};
        $results->{'val'}{$host} = {};
        $results->{'hostvar'}{$host} = {};
    }
    $self->logger->debug( Dumper($results) );  

    foreach my $composite ($composites->get_nodelist) {

        # Get the name of the composite we're scanning as an ID
        my $composite_id = $composite->getAttribute("id");

        # Get <scan> elements from config for oids to scan
        my $scans = $xpc->find("./scan",$composite);

        foreach my $scan ($scans->get_nodelist) {
            # Example Scan: <scan id="ifIdx" oid="1.3.6.1.2.1.31.1.1.1.18.*" var="ifAlias" />

            # Create hash of the basic scan attributes
	        my %scan_attr = (
                scan_id  => $scan->getAttribute("id"),
	            oid      => $scan->getAttribute("oid"),
	            scan_var => $scan->getAttribute("var"),
                ex_only  => $scan->getAttribute("exclude-only"),
            );

            # Add any targets to our scan attributes
	        if ( defined($scan_attr{scan_var}) && defined($params->{$scan_attr{scan_var}}) ) {
                $scan_attr{targets} = $params->{$scan_attr{scan_var}}{"value"};
            }
            
            # Add any exclusion patterns to our scan attributes
            if ( defined($scan_attr{scan_var}) && defined($exclude_patterns{$scan_attr{scan_var}}) ) {
                $scan_attr{excludes} = $exclude_patterns{$scan_attr{scan_var}};
            } else {
                $scan_attr{excludes} = [];
            }

            # Get names/indexes for the variables we need from the scan, and the split oid
            my $scan_map = $self->_map_oid($scan_attr{oid});
            my $split_oid = $scan_map->{split_oid};
            my $trunk = $scan_map->{trunk};
 
            # Add our scan map to the scan results
            $scan_attr{scan_map} = $scan_map;
            $self->logger->debug("Complete Scan Attributes:\n" . Dumper(\%scan_attr));

            # Build the OID tree to scan from its roots to its trunk
            my $scan_oid;
            if ( defined $scan_map ) {
                $scan_oid = join '.', @{$split_oid}[0..$trunk];
            } else {
                # ! Here we can add some backward compatability !
                $scan_oid = join '.', @$split_oid;
            }
            $self->logger->debug($scan_oid);


            $cv->begin;
            # The results from this callback to RabbitMQ are sent directly to _do_vals as $results upon completion
            $self->client->get(
                node            => $hosts, 
                oidmatch        => $scan_oid,
                async_callback  => sub { 
                    my $data = shift;
                    $self->logger->debug(Dumper($data));
                    $self->_scan_cb($data->{'results'},$hosts,$results,\%scan_attr);
                    $cv->end;
                }
            );
        }
    }
    $cv->end;
}


sub _scan_cb {

    my $self      = shift;
    my $data      = shift;
    my $hosts     = shift;
    my $results   = shift;
    my $scan_attr = shift;

    my $scan_map     = $scan_attr->{scan_map};
    my $scan_id      = $scan_attr->{scan_id};
    my $oid_pattern  = $scan_attr->{oid};
    my $targets      = $scan_attr->{targets};
    my $excludes     = $scan_attr->{excludes};
    my $exclude_only = $scan_attr->{ex_only}; # True = Add no results, but possibly blacklist OID values

    for my $host (@$hosts) {
        
        # Track the OIDs we want after exclusions and targets are factored in
        my @oids;

        # Return only entries matching specified value regexps, if value regexps are specified
        my $use_target_matches = (defined($targets) && (scalar(@$targets) > 0));

        # Check our oids and keep only the ones we want
        for my $oid (keys %{$data->{$host}}) {

            my $oid_value = $data->{$host}{$oid}{value};

            # Blacklist the value if it matches an exclude
            if (any { $oid_value =~ /$_/ } @$excludes) {
                $results->{'scan-exclude'}{$host}{$oid} = 1;
            }

            # Skip the OID if the host is exclusion only and is using target matches or the value matches a target
            if ( $exclude_only ) {
                if ( $use_target_matches || !(any { $oid_value =~ /$_/ } @$targets) ) {
                    next;
                }
            }
            # If we didn't exclude the oid, add it to our OIDs to translate
            else {
                push @oids, $oid;  
            }
        }

        # Add the data for the scan to results if we're not excluding the host
        if ( !$exclude_only && @oids ) {
            
            #$self->logger->debug("Translating OIDS for scan: $scan_id"); 
            # Translate the OIDs into a data hash for processing from the data returned by polling the OID base
            my $oid_data = $self->_translate_oids(\@oids, $data->{$host}, $scan_map, 'scan');


            #$results->{scan}{$host}{$scan_id}{oids} = \@oids;
            #$results->{scan}{$host}{$scan_id}{data} = $data->{$host};
            #$results->{scan}{$host}{$scan_id}{map}  = $scan_map;
            $results->{scan}{$host}{$scan_id} = $oid_data;
            #$results->{scan}{$host}{$scan_id}{scan_map} = $scan_map;
        }
    }
    return;
}


# Process and combine the scan results once all of the scans and their callbacks have completed
sub _digest_scans {

    my $self       = shift;
    my $composites = shift; # Top-level XML element for CompData instance
    my $params     = shift; # Parameters to request
    my $results    = shift; # Request-global $results hash
    my $cv         = shift; # Assumes that it's been begin()'ed with a callback

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    $self->logger->debug("Results for _process_scans:\n" . Dumper($results));

    for my $host ( @$hosts ) {

        # Get the scans for the host
        my $scans = $results->{scan}{$host};

        my %combined_scan;
        my %combined_vals;
        my @meta_legend;
        my $meta_scan;

        # Use the results of the scan if it is the only scan;
        if ( scalar(keys %{$scans}) < 2 ) {
            $results->{scan}{$host} = values %{$results->{scan}{$host}};
            next;
        }

        # Otherwise, combine and flatten dependent scan results for the host
        else {

            # Find the scan with the most dependencies and use its legend and vals, adding missing keys for vars
            for my $scan (keys %{$scans}) {
                my $legend = $scans->{$scan}{legend};
                if (! @meta_legend || $#meta_legend < $#$legend) {
                    @meta_legend = @{$legend};
                    $meta_scan   = $scan;
                }
            }

            if ( @meta_legend && $meta_scan) {
                $combined_scan{legend} = \@meta_legend;
                %combined_vals         = %{$scans->{$meta_scan}{vals}}
            }
            else {
                $self->logger->error("No legend was found for any scans");
                return;
            }
            
            my $vals_ref = \%combined_vals;
            
            # Begin combining scan results in order
            for my $scan_id ( @meta_legend ) {

                if ($scan_id eq $meta_scan) { last; }

                # Get the keys of the parent scan;
                my @keys = keys %{$scans->{$scan_id}{vals}};
                for my $key ( @keys ) {
                    if (! exists $vals_ref->{$key} ) {
                        $vals_ref->{$key} = {};
                    }
                }   
            }
        }
        $combined_scan{vals}    = \%combined_vals;
        $results->{scan}{$host} = \%combined_scan;
    }
    $self->logger->debug("Combined scans:\n" . Dumper($results->{scan}));
    $cv->end;
}


# Fetches the host variables and SNMP values for <val> elements
sub _do_vals {
    my $self         = shift;
    my $composites   = shift; # Top-level XML element for CompData instance
    my $params       = shift; # Parameters to request
    my $results      = shift; # Request-global $results hash
    my $cv           = shift; # Assumes that it's been begin()'ed with a callback
    
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

    # Give up on config object and go direct to xmllib to get proper xpath support
    # These should be moved to the constructor
    my $doc = $self->config->{'doc'};
    my $xpc = XML::LibXML::XPathContext->new($doc);
    
    foreach my $composite ($composites->get_nodelist) {

        # Get the <val> elements and loop through them
        my $vals = $xpc->find("./result/val",$composite);
        foreach my $val ($vals->get_nodelist) {

        # Notes for <val> Elements ------------------------------------------
        # The <val> tag can have a couple of different forms:
        #
        # <val id="var_name" var="scan_var_name">
        #     - use a value from the scan phase
        # <val id="var_name" type="rate" oid="1.2.3.4.scan_var_name">
        #     - use OID suffixes from the scan phase, and lookup other OIDs,
        #       optionally doing a rate calculation
        #--------------------------------------------------------------------

            # Get the attributes of the val element
            my %val_attr = (
                id   => $val->getAttribute("id"),
                var  => $val->getAttribute("var"),
                oid  => $val->getAttribute("oid"),
                type => $val->getAttribute("type")
            );

            # Check if the val has an ID	    
            if (!defined $val_attr{id}) {
                $self->logger->error('no ID specified in a <val> element');
                next;
            }

            # Check if the val doesn't have an OID
            if ( !defined $val_attr{oid} ) {

                # Check if the val's OID and var are undefined, skipping the val if true
                if ( !defined $val_attr{var} ) {
                    $self->logger->error("no 'var' param specified for <val id='$val_attr{id}'>");
                    next;
                }
                
                # Check if the val is the special "node" val which is the node name, process, and skip to next val
                # THIS WILL NEED TO BE ADAPTED TO ADD TO THE FINAL DATA STRUCTURE PROPERLY:
                # Basically, for each final data object, add node => $host
                if ( $val_attr{var} eq 'node' && 0 ) {
                    foreach my $host (@$hosts) {
                        my $val_host = $results->{'val'}{$host};
                        $self->logger->debug(Dumper($val_host));     # THIS BLOCK IS MUTED
                        my $scan = $results->{'scan'}{$host};
                        $self->logger->debug(Dumper($scan));
                        foreach my $scan_var (keys %{$scan->{data}}) {
                            foreach my $oid_val (@{$scan->{$scan_var}}) {
                                $val_host->{$oid_val}{$val_attr{id}} = [$host];
                            }
                        }
                    }
                    next;
                }

                # If the val has a defined var attribute, 
                # Set each of that var's scanned oid values in the val hash of results
                # THIS WILL NEED TO BE ADAPTED TO ADD TO THE FINAL DATA STRUCTURE PROPERLY
                foreach my $host (@$hosts) {
                    my $scan_var = $results->{'scan-match'}{$host}{$val_attr{var}};

                    next if !defined($scan_var);

                    foreach my $oid (keys %$scan_var) {
                        #$results->{val}{$host}{$oid_suffix}{$val_attr{id}} = [$scan_var->{$oid_suffix}];
                    }
                }

            # Pull the val's OID data from Simp
            } else {

                # Create a map of the val OID for use
                my $val_map = $self->_map_oid($val_attr{oid});
                next if !(defined $val_map);

                # Add the val's ID to the val_map
                $val_map->{id} = $val_attr{id};

                # Set the position of the trunk of the val OID
                my $trunk   = $val_map->{trunk};

                # Set the base val OID to request from roots to trunk
                my $oid_base = join '.', @{$val_map->{split_oid}}[0..$trunk];

                $self->logger->debug("Set the OID base for \"$val_attr{id}\" with trunk index of $trunk: $oid_base");

                foreach my $host (@$hosts) {

                    # Get the data for these OIDs from Simp
                    $cv->begin;

                    # It is tempting to request just the OIDs you know you want,
                    # instead of asking for the whole subtree, but requesting
                    # a bunch of individual OIDs takes SimpData a *whole* lot
                    # more time and CPU, so we go for the subtree.
                    if ( defined($val_attr{type}) && $val_attr{type} eq 'rate' ) {

                        $self->client->get_rate(
                            node     => [$host],
                            period   => $params->{'period'}{'value'},
                            oidmatch => [$oid_base],
                            async_callback => sub {
                                my $data = shift;
                                $self->_val_cb($data->{'results'},$results,$host,$val_map);
                                $cv->end;
                            }
                        );

                    } else {

                        $self->client->get(
                            node     => [$host],
                            oidmatch => [$oid_base],
                            async_callback => sub {
                                my $data = shift;
                                $self->_val_cb($data->{'results'},$results,$host,$val_map);
                                $cv->end;
                            }
                        );
                    }
                }
            }
        }   # End $val for loop
    }       # End $instance for loop

    $cv->end;
    $self->logger->debug("Ended CV for vals section!");
    $self->logger->debug(Dumper($results->{val})); 
}


sub _hostvar_cb {

    my $self    = shift;
    my $data    = shift;
    my $results = shift;

    foreach my $host (keys %$data) {
        foreach my $oid (keys %{$data->{$host}}) {
            my $val = $data->{$host}{$oid}{'value'};
            $self->logger->debug(Dumper($val));
            $oid =~ s/^vars\.//;
            $results->{'hostvar'}{$host}{$oid} = $val;
        }
    }
}


# Callback to get data for a val
sub _val_cb {

    my $self      = shift;
    my $data      = shift;
    my $results   = shift;
    my $host      = shift;
    my $val_map   = shift;

    # Get the scan data for the host
    my $scan_data = $results->{scan}{$host};

    $self->logger->debug("Running _val_cb");

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

    # Get the translated data for the val using the wanted OIDs
    my $val_data = $self->_translate_oids(\@oids, $data->{$host}, $val_map);

    $self->logger->debug(Dumper($val_data));

    # Check translated data, removing leaves and branches that were not wanted
    $val_data = $self->_trim_tree($val_data->{vals}, $scan_data->{vals});

    # Add the translated, cleaned data to to the val results for the host, at the val_id
    $results->{val}{$host}{$val_map->{id}} = $val_data;

    return;
}

# Adds all value leaves of a val_tree to the data_tree's hash leaves
sub _grow_tree {
    my $self      = shift;
    my $val_id    = shift;
    my $val_tree  = shift;
    my $data_tree = shift;

    $self->logger->debug("_grow_tree checking values for $val_id in:\n" . Dumper($data_tree));

    if ( (! keys $data_tree && ref($data_tree) eq ref({})) || exists $data_tree->{time} ) {
        $self->logger->debug("Passed in data tree is a data leaf");
        if ( exists $val_tree->{value} ) {
            $data_tree->{$val_id} = $val_tree->{value};
            $self->logger->debug("Setting the value for the data leaf");
            if ( !exists $data_tree->{time} ) {
                $data_tree->{time} = $val_tree->{time};
            }
        }
        return;
    }
            

    # Loop over the all the relevant keys of the data tree
    for my $key ( keys %{$data_tree} ) {

        $self->logger->debug("$val_id: Checking key $key");

        # The data values haven't been reached yet
        if ( !exists $val_tree->{time} && !exists $val_tree->{value} ) {

            # Check that the val_tree follows the path along the data tree
            if ( exists $val_tree->{$key} ) {

                $self->logger->debug("Going deeper into both trees for $key to: $val_tree->{$key}<->$data_tree->{$key}");

                # Recurse with the new key in both hashes
                $self->_grow_tree($val_id, $val_tree->{$key}, $data_tree->{$key});
            }
        }
        # The data values have been reached
        else {

            $self->logger->debug("Have data values");
            # Key is pointing to an empty result data hash
            if ( ref($data_tree->{$key}) eq ref({}) && !keys $data_tree->{$key} ) {
                $self->logger->debug("Found empty data hash in data_tree at $key, adding values:");
                # Assign the data for the val, setting time with the first val
                $data_tree->{$key}{time}    = $val_tree->{time};
                $data_tree->{$key}{$val_id} = $val_tree->{value};
                $self->logger->debug(Dumper($data_tree->{$key}));
            }
            # Check if any keys at the next level go deeper
            elsif ( !exists $data_tree->{time} ) { #any { ref($_) eq ref({}) } (keys %{$data_tree->{$key}}) ) {
                # Recurse deeper into data_tree, keeping the val_tree on level with its vals
                $self->logger->debug("data_tree has more branches/leaves at $key, recursing for vals:\n" . Dumper($val_tree));
                $self->_grow_tree($val_id, $val_tree, $data_tree->{$key});
            }
            # If we've hit the values and the last data_tree branch with levels
            else {
                # Assign the data for the val
                $self->logger->debug("Hit final leaf of data_tree at $key, assigning it $val_tree->{value}");
                $data_tree->{$key}{$val_id} = $val_tree->{value};
            }
        }
    }
    return;
}

# Digests the val data into data output after all callbacks have completed
sub _digest_vals {

    my $self       = shift;
    my $composites = shift; # Top-level XML element for CompData instance
    my $params     = shift; # Parameters to request
    my $results    = shift; # Request-global $results hash
    my $cv         = shift; # Assumes that it's been begin()'ed with a callback

    $self->logger->debug("Data passed to _digest_vals:\n" . Dumper($results));

    # Get the array of hosts from params
    my $hosts = $params->{'node'}{'value'};

    for my $host ( @$hosts ) {

        # Create a data hash for the host vals from its scan vals tree
        my %data_tree = %{clone($results->{scan}{$host}{vals})};
        
        # Get the vals polled for the host
        my $vals = $results->{val}{$host};

        for my $val ( keys %{$vals} ) {
            $self->logger->debug("\n\n\nNow adding data values for $val to the data tree");
            $self->_grow_tree($val, $vals->{$val}, \%data_tree);
            #last; #do one for testing;
        }

        $results->{val}{$host} = \%data_tree;
    }
    $self->logger->debug(Dumper($results));
    $cv->end;
}


# Applies functions to values gathered by _do_vals
sub _do_functions {

    my $self    = shift;
    my $xrefs   = shift; # top-level XML element for CompData instance
    my $params  = shift; # parameters to request
    my $results = shift; # request-global $results hash
    my $cv      = shift; # assumes that it's been begin()'ed with a callback

    #$self->logger->debug(Dumper($results));

    my $now = time;

    # First off, by default, we pass through the 'time' value, as it has special
    # significance for clients:
    foreach my $host (keys %{$results->{'val'}}) {
        foreach my $oid_suffix (keys %{$results->{'val'}{$host}}) {
            my $tm = $results->{'val'}{$host}{$oid_suffix}{'time'};
            $tm = $now if !defined($tm);
            $results->{'final'}{$host}{$oid_suffix}{'time'} = $tm;
        }
    }

    my $xpc = XML::LibXML::XPathContext->new($self->config->{'doc'});

    my $vals = $xpc->find("./result/val", $xrefs->get_nodelist);
    foreach my $val ($vals->get_nodelist) {
        my $val_name = $val->getAttribute("id");
        my @fctns    = $xpc->find("./fctn", $val)->get_nodelist;
        $self->_function_one_val($val_name, \@fctns, $params, $results);
    }

    # Finally, filter out OID suffixes that we identified
    # as "should be excluded" in the scan phase
    foreach my $host (keys %{$results->{'val'}}) {
        foreach my $oid_suffix (keys %{$results->{'val'}{$host}}) {
            if ($results->{'scan-exclude'}{$host}{$oid_suffix}) {
                delete $results->{'final'}{$host}{$oid_suffix};
            }
        }
    }

    $cv->end;
}


# Run functions for one of the <val>s defined for this instance
sub _function_one_val {

    my $self     = shift;
    my $val_name = shift; # name of the value to apply functions to
    my $fctns    = shift; # list of <fctn> elements
    my $params   = shift;
    my $results  = shift;

    my $have_run_warning = 0;

    # Iterate over all (host, OID suffix) pairs in the retrieved values
    foreach my $host (keys %{$results->{'val'}}) {
        foreach my $oid_suffix (keys %{$results->{'val'}{$host}}) {
            my $val_set = $results->{'val'}{$host}{$oid_suffix};
            my $val = $val_set->{$val_name};

            # Apply all functions defined for the value to it, in order:
            foreach my $fctn (@$fctns) {
                my $func_id = $fctn->getAttribute('name');
                if (!defined($_FUNCTIONS{$func_id})) {
                    $self->logger->error("Unknown function name \"$func_id\" for val \"$val_name\"!") if !$have_run_warning;
                    $have_run_warning = 1;
                    $val = undef;
                    last;
                }

                # Fetch a commonly-used attribute
                my $operand = $fctn->getAttribute("value");
                warn "Function: " . $func_id . "\n";
		        $val = $_FUNCTIONS{$func_id}($val, $operand, $fctn, $val_set, $results, $host);
		        warn "VAL: " . Dumper($val);
            }
	    
	        $val = $val->[0];
            $results->{'final'}{$host}{$oid_suffix}{$val_name} = $val;
        }
    }
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
	warn "Passed in Vals: " . Dumper($vals);
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
    'regexp' => sub { # regular-expression match and extract first group
        my ($vals, $operand) = @_;
	foreach my $val (@$vals){
	    if($val =~ /$operand/){
		return [$1];
	    }
	    return [$val];
	}
    },
    'replace' => sub { # regular-expression replace
        my ($vals, $operand, $elem) = @_;
	foreach my $val (@$vals){
	    my $replace_with = $elem->getAttribute("with");
	    $val = Data::Munge::replace($val, $operand, $replace_with);
	    return [$val];
	}
    },
    'rpn' => sub { return [ _rpn_calc(@_) ]; },
);

sub _rpn_calc{
    my ($vals, $operand, $fctn_elem, $val_set, $results, $host) = @_;
    warn "RPN CALC: " . Dumper($vals, $val_set);
    foreach my $val (@$vals){
	# As a convenience, we initialize the stack with a copy of $val on it already
	my @stack = ($val);
	
	# Split the RPN program's text into tokens (quoted strings,
	# or sequences of non-space chars beginning with a non-quote):
	my @prog;
	my $progtext = $operand;
	while (length($progtext) > 0){
	    $progtext =~ /^(\s+|[^\'\"][^\s]*|\'([^\'\\]|\\.)*(\'|\\?$)|\"([^\"\\]|\\.)*(\"|\\?$))/;
	    my $x = $1;
	    push @prog, $x if $x !~ /^\s*$/;
	    $progtext = substr $progtext, length($x);
	}
	
	my %func_lookup_errors;
	my @prog_copy = @prog;
	GRNOC::Log::log_debug('RPN Program: ' . Dumper(\@prog_copy));
	
	# Now, go through the program, one token at a time:
	foreach my $token (@prog){
	    # Handle some special cases of tokens:
	    if($token =~ /^[\'\"]/){ # quoted strings
		# Take off the start and end quotes, including
		# the handling of unterminated strings:
		if($token =~ /^\"/) {
		    $token =~ s/^\"(([^\"\\]|\\.)*)[\"\\]?$/$1/;
		}else{
		    $token =~ s/^\'(([^\'\\]|\\.)*)[\'\\]?$/$1/;
		}
		$token =~ s/\\(.)/$1/g; # unescape escapes
		push @stack, $token;
	    }elsif($token =~ /^[+-]?([0-9]+\.?|[0-9]*\.[0-9]+)$/){ # decimal numbers
		push @stack, ($token + 0);
	    }elsif($token =~ /^\$/){ # name of a value associated with the current (host, OID suffix)
		push @stack, $val_set->{substr $token, 1}->[0];
	    }elsif($token =~ /^\#/){ # host variable
		push @stack, $results->{'hostvar'}{$host}{substr $token, 1};
	    }elsif($token eq '@'){ # push hostname
		push @stack, $host;
	    }else{ # treat as a function
		if (!defined($_RPN_FUNCS{$token})){
		    GRNOC::Log::log_error("RPN function $token not defined!") if !$func_lookup_errors{$token};
		    $func_lookup_errors{$token} = 1;
		    next;
            }
		$_RPN_FUNCS{$token}(\@stack);
	    }
	    
	    # We copy, as in certain cases Dumper() can affect the elements of values passed to it
	    my @stack_copy = @stack;
	    GRNOC::Log::log_debug("Stack, post token '$token': " . Dumper(\@stack_copy));
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
