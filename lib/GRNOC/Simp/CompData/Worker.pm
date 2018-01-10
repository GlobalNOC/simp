package GRNOC::Simp::CompData::Worker;

use strict;
use Carp;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Data::Munge qw();
use Try::Tiny;
use Moo;
use Redis;
use AnyEvent;
use GRNOC::Log;
use GRNOC::RabbitMQ::Method;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Client;
use GRNOC::WebService::Regex;


### required attributes ###

has config => ( is => 'ro',
                required => 1 );

has logger => ( is => 'ro',
                required => 1 );

has worker_id => ( is => 'ro',
               required => 1 );


### internal attributes ###

has is_running => ( is => 'rwp',
                    default => 0 );

has dispatcher  => ( is => 'rwp' );

has client      => ( is => 'rwp' );

has need_restart => (is => 'rwp',
                    default => 0 );

my %_FUNCTIONS; # Used by _function_one_val
my %_RPN_FUNCS; # Used by _rpn_calc

### public methods ###
sub start {
   my ( $self ) = @_;

  while(1){
    #--- we use try catch to, react to issues such as com failure
    #--- when any error condition is found, the reactor stops and we then reinitialize 
    $self->logger->debug( $self->worker_id." restarting." );
    $self->_start();
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
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info( "Received SIG HUP." );
    };

    my $rabbit_host = $self->config->get( '/config/rabbitMQ/@host' );
    my $rabbit_port = $self->config->get( '/config/rabbitMQ/@port' );
    my $rabbit_user = $self->config->get( '/config/rabbitMQ/@user' );
    my $rabbit_pass = $self->config->get( '/config/rabbitMQ/@password' );
 
    $self->logger->debug( 'Setup RabbitMQ' );

    my $client = GRNOC::RabbitMQ::Client->new(   host => "127.0.0.1",
                                             port => 5672,
                                             user => "guest",
                                             pass => "guest",
                                             exchange => 'Simp',
                                             timeout => 15,
                                             topic => 'Simp.Data');

    $self->_set_client($client);

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new( 	queue_name => "Simp.CompData",
							topic => "Simp.CompData",
							exchange => "Simp",
							user => $rabbit_user,
							pass => $rabbit_pass,
							host => $rabbit_host,
							port => $rabbit_port);

    #--- parse config and create methods based on the set of composite definitions.
    $self->config->{'force_array'} = 1; 
    my $allowed_methods = $self->config->get( '/config/composite' );

    foreach my $meth (@$allowed_methods){
      my $method_id = $meth->{'id'};
      print "$method_id:\n";

      my $method = GRNOC::RabbitMQ::Method->new(  name => "$method_id",
						  async => 1,
                                                  callback =>  sub {$self->_get($method_id,@_) },
                                                  description => "retrieve composite simp data of type $method_id, we should add a descr to the config");

      $method->add_input_parameter( name => 'node',
				    description => 'nodes to retrieve data for',
				    required => 1,
				    multiple => 1,
				    pattern => $GRNOC::WebService::Regex::TEXT);

      $method->add_input_parameter( name => 'period',
				    description => "period of time to request for the data!",
				    required => 0,
				    multiple => 0,
				    pattern => $GRNOC::WebService::Regex::ANY_NUMBER);

      #--- let xpath do the iteration for us
      my $path = "/config/composite[\@id=\"$method_id\"]/input";
      my $inputs = $self->config->get($path);
      foreach my $input (@$inputs){
        my $input_id = $input->{'id'};
        next if ($input_id eq 'node') || ($input_id eq 'period');
        my $required = 0;
        if(defined $input->{'required'}){$required = 1;}

        $method->add_input_parameter( name => $input_id,
				      description => "we will add description to the config file later",
				      required => $required,
				      multiple => 1,
				      pattern => $GRNOC::WebService::Regex::TEXT);
        

        print "  $input_id: $required:\n";
      }
      $dispatcher->register_method($method);
    }

    $self->config->{'force_array'} = 0;

    #--------------------------------------------------------------------------

    my $method2 = GRNOC::RabbitMQ::Method->new(  name => "ping",
                                                callback =>  sub { $self->_ping() },
                                                description => "function to test latency");

    $dispatcher->register_method($method2);
 
    #--- go into event loop handing requests that come in over rabbit  
    $self->logger->debug( 'Entering RabbitMQ event loop' );
    $dispatcher->start_consuming();
    
    #--- you end up here if one of the handlers called stop_consuming
    return;
}

### private methods ###

sub _ping{
  my $self = shift;
  return gettimeofday();
}

sub _get{
  my $self      = shift;
  my $composite = shift;
  my $rpc_ref   = shift;
  my $params    = shift;

  if(!defined($params->{'period'}{'value'})){
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


  #--- because we have to do things asynchronously, execution from here follows
  #--- a series of callbacks, tied together using the $cv[*] condition variables:
  #--- _do_scans       -> _do_vals       -> _do_functions -> success
  #---     \->_scan_cb      | \->_val_cb
  #---                      \->_hostvar_cb

  # Data is accumulated in the %results hash, which has the following structure:
  #
  # $results{'scan'}{$node}{$var_name} = [ list of OID suffixes ]
  #    * The results from the scan phase (_do_scans and _scan_cb)
  # $results{'scan-match'}{$node}{$var_name}{$oid_suffix} = $val
  #    * Mapping from (scan-variable name, OID suffix) to value at OID
  # $results{'val'}{$host}{$oid_suffix}{$var_name} = $val
  #    * The results from the get-values phase (_do_vals and _val_cb)
  # $results{'hostvar'}{$host}{$hostvar_name} = $val
  #    * The host variables (_do_vals and _hostvar_cb)
  # $results{'final'}{$host}{$oid_suffix}{$var_name} = $val
  #    * The results from the compute-functions phase (_do_functions);
  #      $results{'final'} is passed back to the caller

  my $success_callback = $rpc_ref->{'success_callback'};


  my @cv = map { AnyEvent->condvar; } (0..3);

  $cv[0]->begin(sub { $self->_do_scans($ref, $params, \%results, $cv[1]); });
  $cv[1]->begin(sub { $self->_do_vals($ref, $params, \%results, $cv[2]); });
  $cv[2]->begin(sub { $self->_do_functions($ref, $params, \%results, $cv[3]); });
  $cv[3]->begin(sub { \&$success_callback($results{'final'}); });

  # Start off the pipeline:
  $cv[0]->end;
}

sub _do_scans{
  my $self         = shift;
  my $xrefs        = shift; # top-level XML element for CompData instance
  my $params       = shift; # parameters to request
  my $results      = shift; # request-global $results hash
  my $cv           = shift; # assumes that it's been begin()'ed with a callback


  #--- find the set of required variables
  my $hosts = $params->{'node'}{'value'};
  
  #--- this function will execute multiple scans in "parallel" using the begin / end approach
  #--- we use $cv to signal when all those scans are done
  
  #--- give up on config object and go direct to xmllib to get proper xpath support
  #--- these should be moved to the constructor
  my $doc = $self->config->{'doc'};
  my $xpc = XML::LibXML::XPathContext->new($doc);

  # Make sure several root hashes exist
  foreach my $host (@$hosts){
      $results->{'scan'}{$host} = {};
      $results->{'scan-match'}{$host} = {};
      $results->{'val'}{$host} = {};
      $results->{'hostvar'}{$host} = {};
  }
 
  foreach my $instance ($xrefs->get_nodelist){
      my $instance_id = $instance->getAttribute("id");
      #--- get the list of scans to perform
      my $scanres = $xpc->find("./scan",$instance);
      foreach my $scan ($scanres->get_nodelist){
          # example scan:
          # <scan id="ifIdx" oid="1.3.6.1.2.1.31.1.1.1.18.*" var="ifAlias" />
	  my $var_name = $scan->getAttribute("id");
	  my $oid      = $scan->getAttribute("oid");
	  my $param_nm = $scan->getAttribute("var");
	  my $targets;
	  if(defined($param_nm) && defined($params->{$param_nm})){
	      $targets = $params->{$param_nm}{"value"};
	  }
	  $cv->begin;

	  $self->client->get(
	      node => $hosts, 
	      oidmatch => $oid,
	      async_callback => sub {
		  my $data = shift;
		  $self->_scan_cb($data->{'results'},$hosts,$var_name,$oid,$targets,$results); 
		  $cv->end;
	      } );
      }
  }
  $cv->end;
}

sub _scan_cb{
  my $self        = shift;
  my $data        = shift;
  my $hosts       = shift;
  my $var_name    = shift;
  my $oid_pattern = shift;
  my $vals        = shift;
  my $results     = shift;

  $oid_pattern =~ s/\*.*$//;
  $oid_pattern =  quotemeta($oid_pattern);
  
  foreach my $host (@$hosts){

      my @oid_suffixes;

      # return only those entries matching specified values, if values are specified
      my %val_matches;
      my $use_val_matches = 0;
      if(defined($vals) && (scalar(@$vals) > 0)){
          $use_val_matches = 1;
          %val_matches = map { $_ => 1 } @$vals;
      }

      foreach my $oid (keys %{$data->{$host}}){
	  my $base_value = $data->{$host}{$oid}{'value'};

          # strip out the wildcard part of the oid
	  $oid =~ s/^$oid_pattern//;

          if((!$use_val_matches) || $val_matches{$base_value}){
              push @oid_suffixes, $oid;
              $results->{'scan-match'}{$host}{$var_name}{$oid} = $base_value;
          }
      }

      $results->{'scan'}{$host}{$var_name} = \@oid_suffixes;
  }
  
  return ;
}

# Fetches the host variables and SNMP values for <val> elements
sub _do_vals{
    my $self         = shift;
    my $xrefs        = shift; # top-level XML element for CompData instance
    my $params       = shift; # parameters to request
    my $results      = shift; # request-global $results hash
    my $cv           = shift; # assumes that it's been begin()'ed with a callback
    
    #--- find the set of required variables
    my $hosts = $params->{'node'}{'value'};
    
    #--- this function will execute multiple gets in "parallel" using the begin / end apprach
    #--- we use $cv to signal when all those gets are done

    # Get host variables
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

    #--- give up on config object and go direct to xmllib to get proper xpath support
    #--- these should be moved to the constructor
    my $doc = $self->config->{'doc'};
    my $xpc = XML::LibXML::XPathContext->new($doc);
    
    foreach my $instance ($xrefs->get_nodelist){
	#--- get the list of scans to perform
	my $valres = $xpc->find("./result/val",$instance);
	foreach my $val ($valres->get_nodelist){
            # The <val> tag can have a couple of different forms:
            #
            # <val id="var_name" var="scan_var_name">
            #     - use a value from the scan phase
            # <val id="var_name" type="rate" oid="1.2.3.4.scan_var_name">
            #     - use OID suffixes from the scan phase, and lookup other OIDs,
            #       optionally doing a rate calculation
	    my $id      = $val->getAttribute("id");
	    my $var     = $val->getAttribute("var");
	    my $oid     = $val->getAttribute("oid");
	    my $type    = $val->getAttribute("type");
	    
	    if(!defined $id){
		#--- required data missing
                $self->logger->error('no ID specified in a <val> element');
		next;
	    }

            if(!defined $oid){ # Use the results of a scan
                if(!defined $var){
                    $self->logger->error("no 'var' param specified for <val id='$id'>");
                    next;
                }


                if($var eq 'node'){
                    # special case: use the node name instead
                    foreach my $host (@$hosts){
                        my $val_host = $results->{'val'}{$host};
                        my $scan = $results->{'scan'}{$host};
                        foreach my $scan_var (keys %{$scan}){
                            foreach my $oid_suffix (@{$scan->{$scan_var}}){
                                $val_host->{$oid_suffix}{$id} = $host;
                            }
                        }
                    }
                    next;
                }

                foreach my $host (@$hosts){
                    my $val_host = $results->{'val'}{$host};
                    my $scan_var = $results->{'scan-match'}{$host}{$var};

                    next if !defined($scan_var);

                    foreach my $oid_suffix (keys %$scan_var){
                        $val_host->{$oid_suffix}{$id} = $scan_var->{$oid_suffix};
                    }
                }
            }else{ # pull data from Simp
                # fetch the scan-variable name to use:
                my @oid_parts = split /\./, $oid;
                my $scan_var_idx = 0;

                while ($scan_var_idx < scalar @oid_parts){
                    last if !($oid_parts[$scan_var_idx] =~ /^[0-9]*$/);
                    $scan_var_idx += 1;
                }
                if ($scan_var_idx >= scalar @oid_parts){
                    $self->logger->error("no scan-variable name found for <val id='$id'>");
                    next;
                }

                my $scan_var_name = $oid_parts[$scan_var_idx];

                foreach my $host (@$hosts) {
                    my $oid_suffixes = $results->{'scan'}{$host}{$scan_var_name};
                    next if !defined($oid_suffixes); # Make sure there's stuff to iterate over

                    my %lut; # look-up table from (full OID) to list of (OID suffix, variable name) pairs
                    my @oid_list;

                    foreach my $oid_suffix (@$oid_suffixes){
                        # re-use @oid_parts to construct the full OID to look for
                        $oid_parts[$scan_var_idx] = $oid_suffix;
                        my $full_oid = join '.', @oid_parts;

                        push @oid_list, $full_oid;
                        push @{$lut{$full_oid}}, [$oid_suffix, $id];
                    }

                    # SimpData does not like it when you call it with an empty oidmatch, so:
                    next if scalar(@oid_list) <= 0;

                    # Now get the data for these OIDs from Simp
                    $cv->begin;

                    if(defined($type) && $type eq 'rate'){
                        $self->client->get_rate(
                            node     => [$host],
                            period   => $params->{'period'}{'value'},
                            oidmatch => \@oid_list,
                            async_callback => sub {
                                my $data = shift;
				$self->_val_cb($data->{'results'},$results,$host,\%lut);
				$cv->end;
                            }
                        );
                    }else{
                        $self->client->get(
                            node     => [$host],
                            oidmatch => \@oid_list,
                            async_callback => sub {
                                my $data = shift;
				$self->_val_cb($data->{'results'},$results,$host,\%lut);
				$cv->end;
                            }
                        );
                    }
                }
            }
	}
    }
    $cv->end; 
}

sub _hostvar_cb{
  my $self    = shift;
  my $data    = shift;
  my $results = shift;

  foreach my $host (keys %$data){
      foreach my $oid (keys %{$data->{$host}}){
          my $val = $data->{$host}{$oid}{'value'};
          $oid =~ s/^vars\.//;
          $results->{'hostvar'}{$host}{$oid} = $val;
      }
  }
}

sub _val_cb{
  my $self        = shift;
  my $data        = shift;
  my $results     = shift;
  my $host        = shift;
  my $lut         = shift;

  return if !defined($data->{$host});

  foreach my $oid (keys %{$data->{$host}}){
      my $val      = $data->{$host}{$oid}{'value'};
      my $val_time = $data->{$host}{$oid}{'time'};

      my $indices = $lut->{$oid};
      next if !defined($indices);

      foreach my $index (@$indices){
          $results->{'val'}{$host}{$index->[0]}{$index->[1]} = $val;
          if(!defined($results->{'val'}{$host}{$index->[0]}{'time'})){
              $results->{'val'}{$host}{$index->[0]}{'time'} = $val_time;
          }
      }
  }

  return;
}

# Applies functions to values gathered by _do_vals
sub _do_functions{
    my $self         = shift;
    my $xrefs        = shift; # top-level XML element for CompData instance
    my $params       = shift; # parameters to request
    my $results      = shift; # request-global $results hash
    my $cv           = shift; # assumes that it's been begin()'ed with a callback

    # First off, by default, we pass through the 'time' value, as it has special
    # significance for clients:
    foreach my $host (keys %{$results->{'val'}}){
        foreach my $oid_suffix (keys %{$results->{'val'}{$host}}){
            $results->{'final'}{$host}{$oid_suffix}{'time'} = $results->{'val'}{$host}{$oid_suffix}{'time'};
        }
    }

    my $xpc = XML::LibXML::XPathContext->new($self->config->{'doc'});

    my $vals = $xpc->find("./result/val", $xrefs->get_nodelist);
    foreach my $val ($vals->get_nodelist){
        my $val_name = $val->getAttribute("id");
        my @fctns    = $xpc->find("./fctn", $val)->get_nodelist;

        $self->_function_one_val($val_name, \@fctns, $params, $results);
    }

    $cv->end;
}

# Run functions for one of the <val>s defined for this instance
sub _function_one_val{
    my $self     = shift;
    my $val_name = shift; # name of the value to apply functions to
    my $fctns    = shift; # list of <fctn> elements
    my $params   = shift;
    my $results  = shift;

    my $have_run_warning = 0;

    # Iterate over all (host, OID suffix) pairs in the retrieved values
    foreach my $host (keys %{$results->{'val'}}){
        foreach my $oid_suffix (keys %{$results->{'val'}{$host}}){
            my $val_set = $results->{'val'}{$host}{$oid_suffix};
            my $val = $val_set->{$val_name};

            # Apply all functions defined for the value to it, in order:
            foreach my $fctn (@$fctns){
                my $func_id = $fctn->getAttribute('name');
                if (!defined($_FUNCTIONS{$func_id})) {
                    $self->logger->error("Unknown function name \"$func_id\" for val \"$val_name\"!") if !$have_run_warning;
                    $have_run_warning = 1;
                    $val = undef;
                    last;
                }

                # Fetch a commonly-used attribute
                my $operand = $fctn->getAttribute("value");
                $val = $_FUNCTIONS{$func_id}($val, $operand, $fctn, $val_set, $results, $host);
            }

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
    '+' => sub { # addition
        my ($val, $operand) = @_;
        return $val if !defined($val);
        $val + $operand;
    },
    '-' => sub { # subtraction
        my ($val, $operand) = @_;
        return $val if !defined($val);
        $val - $operand;
    },
    '*' => sub { # multiplication
        my ($val, $operand) = @_;
        return $val if !defined($val);
        $val * $operand;
    },
    '/' => sub { # division
        my ($val, $operand) = @_;
        return $val if !defined($val);
        $val / $operand;
    },
    '%' => sub { # modulus
        my ($val, $operand) = @_;
        return $val if !defined($val);
        $val % $operand;
    },
    'ln' => sub { # base-e logarithm
        my $val = shift;
        return $val if !defined($val);
        eval { log($val); }; # if val==0, we want the result to be undef, so this works just fine
    },
    'log10' => sub { # base-10 logarithm
        my $val = shift;
        return $val if !defined($val);
        $val = eval { log($val); }; # see ln
        $val /= log(10) if defined($val);
        $val;
    },
    'regexp' => sub { # regular-expression match and extract first group
        my ($val, $operand) = @_;
        if($val =~ /$operand/){
            return $1;
        }
        $val;
    },
    'replace' => sub { # regular-expression replace
        my ($val, $operand, $elem) = @_;
        my $replace_with = $elem->getAttribute("with");
        $val = Data::Munge::replace($val, $operand, $replace_with);
        $val;
    },
    'rpn' => \&_rpn_calc,
);

sub _rpn_calc{
    my ($val, $operand, $fctn_elem, $val_set, $results, $host) = @_;

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
            push @stack, $val_set->{substr $token, 1};
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
        my $res = (!(defined($a) xor defined($b))) ? ($a == $b) : 0;
        push @$stack, _bool_to_int($res);
    },
    # a b => (is a numerically unequal to b?)
    '!=' => sub {
        my $stack = shift;
        my $b = pop @$stack;
        my $a = pop @$stack;
        my $res = (!(defined($a) xor defined($b))) ? ($a != $b) : 1;
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
