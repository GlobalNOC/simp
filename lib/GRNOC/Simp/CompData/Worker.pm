package GRNOC::Simp::CompData::Worker;

use strict;
use Carp;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Try::Tiny;
use Moo;
use Redis;
use AnyEvent;
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
  my $xc  = XML::LibXML::XPathContext->new($doc);

  #--- get the instance
  my $path = "/config/composite[\@id=\"$composite\"]/instance[\@hostType=\"$hostType\"]";
  my $ref = $xc->find($path);


  #--- because we have to do things asynchronously, execution from here follows
  #--- a series of callbacks, tied together using the $cv[*] condition variables:
  #--- _do_scans       -> _do_vals       -> _do_functions -> success
  #---     \->_scan_cb      | \->_val_cb
  #---                      \->_hostvar_cb

  # Data is accumulated in the %results hash, which has the following structure:
  #
  # $results{'scan'}{$node}{$var_name} = [ list of OID suffixes ]
  #    * The results from the scan phase (_do_scans and _scan_cb)
  # $results{'val'}{$host}{$oid_suffix}{$var_name} = $val
  #    * The results from the get-values phase (_do_vals and _val_cb)
  # $results{'hostvar'}{$host}{$hostvar_name} = $val
  #    * The host variables (_hostvar_cb)
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
  my $xrefs        = shift;
  my $params       = shift;
  my $results      = shift;
  my $cv           = shift; # assumes that it's been begin()'ed with a callback


  #--- find the set of required variables
  my $hosts = $params->{'node'}{'value'};
  
  #--- this function will execute multiple scans in "parallel" using the begin / end apprach
  #--- we use $cv to signal when all those scans are done
  
  #--- give up on config object and go direct to xmllib to get proper xpath support
  #--- these should be moved to the constructor
  my $doc = $self->config->{'doc'};
  my $xc  = XML::LibXML::XPathContext->new($doc);
 
  foreach my $instance ($xrefs->get_nodelist){
      my $instance_id = $instance->getAttribute("id");
      #--- get the list of scans to perform
      my $scanres = $xc->find("./scan",$instance);
      foreach my $scan ($scanres->get_nodelist){
	  my $id      = $scan->getAttribute("id");
	  my $oid     = $scan->getAttribute("oid");
	  my $var     = $scan->getAttribute("var");
	  my $targets;
	  if(defined $var){
	      $targets = $params->{$var}{"value"};
	  }
	  $cv->begin;

	  $self->client->get(
	      node => $hosts, 
	      oidmatch => $oid,
	      async_callback => sub {
		  my $data= shift;
		  $self->_scan_cb($data->{'results'},$hosts,$id,$oid,$targets,$results); 
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
  my $id          = shift;
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
      if(defined($vals) && scalar(@$vals) > 0){
          $use_val_matches = 1;
          %val_matches = map { $_ => 1 } @$vals;
      }

      foreach my $oid (keys %{$data->{$host}}){
	  my $base_value = $data->{$host}{$oid}{'value'};
	  
          # strip out the wildcard part of the oid
	  $oid =~ s/^$oid_pattern//;
	  
          #--- return only those entries matching specified values
          if($use_val_matches){
              push @oid_suffixes, $oid if $val_matches($base_value);
          }
      }

      $results->{'scan'}{$host}{$id} = \@oid_suffixes;
  }
  
  return ;
}

sub _do_vals{
    my $self         = shift;
    my $xrefs        = shift;
    my $params       = shift;
    my $results      = shift;
    my $cv           = shift; # assumes that it's been begin()'ed with a callback
    
    #--- find the set of required variables
    my $hosts = $params->{'node'}{'value'};
    
    
    #--- this function will execute multiple gets in "parallel" using the begin / end apprach
    #--- we use $cv to signal when all those gets are done
    
    #--- give up on config object and go direct to xmllib to get proper xpath support
    #--- these should be moved to the constructor
    my $doc = $self->config->{'doc'};
    my $xc  = XML::LibXML::XPathContext->new($doc);
    
    foreach my $instance ($xrefs->get_nodelist){
	#--- get the list of scans to perform
	my $valres = $xc->find("./result/val",$instance);
	foreach my $val ($valres->get_nodelist){
	    my $id      = $val->getAttribute("id");
	    my $var     = $val->getAttribute("var");
	    my $oid     = $val->getAttribute("oid");
	    my $type    = $val->getAttribute("type");
	    
	    
	    if(!defined $var || !defined $id){
		#--- required data missing
		$self->logger->error("NO VAR OR ID Specified");
		next;
	    }
	    
	    if(!defined $oid){
		$self->logger->error("NO OID Specified! Just appending vars!");
		my %data;
		foreach my $host(@$hosts){
		    foreach my $key (keys %{$results->{$host}{$var}}){
			$self->logger->error("Processing ID: " . $id . " with VAR: " . $var . " with key: " . $key . " and value: " . $results->{$host}{$var}{$key});

			if(!defined($results->{'final'}{$host}{$key})){
			    $results->{'final'}{$host}{$key} = {};
			}
			
			

			$self->_do_functions_old(values => [$results->{$host}{$var}{$key}],
					     var => $var,
					     xpath => $val,
					     results => $results->{'final'}{$host}{$key},
					     id => $id);
			#$results->{'final'}{$host}{$key}{$id} = $self->_do_functions_old( value => $results->{$host}{$var}{$key},
			#							      xpath => $xref, 
			#							      results => $results->{'final'}{$host}{$key}{$id});
		    }
		}
		next;
	    }
	    
	    #--- we need pull data from simp 
	    foreach my $host(@$hosts){
		my @matches;
		my @hostarray;
		my %lut;
		
		#--- each host gets its own array of match patterns
		#--- as thse are very specific

		my $ref = $results->{$host}{$var};
		push(@hostarray,$host);
		if(scalar(keys %{$ref}) == 1){
		    
		    foreach my $key (keys %{$ref}){
			my $val = $ref->{$key};
			my $match = $oid;
			$match =~ s/$var/$val/;
			$lut{$match} = $key;
			push(@matches,$match);
		    }
		    
		    #if there are no matches for this host
		    #just go on to the next one!
		    next if(scalar(@matches) <= 0);
		    
		    #--- send the array of matches to simp
		    $cv->begin;
		    
		    if(defined $type && $type eq "rate"){
			$self->client->get_rate(
			    node => \@hostarray,
			    period => $params->{'period'}{'value'},
			    oidmatch => \@matches,
			    async_callback =>  sub {
				my $data= shift;
				$self->_val_cb($data->{'results'},$results,$host,$id,\%lut,$val);
				$cv->end;
			    } );
			
			
		    }else{
			$self->client->get(
			    node => \@hostarray, 
			    oidmatch => \@matches,
			    async_callback =>  sub {
				my $data= shift; 
				$self->_val_cb($data->{'results'},$results,$host,$id,\%lut,$val); 
				$cv->end;
			    } );      
			
		    }
		}else{
		    #do an optimized search!
		    my $match = $oid;
		    $match =~ s/$var/\*/;
		    #--- send the array of matches to simp
                    $cv->begin;

		    foreach my $key (keys %{$ref}){
                        my $val = $ref->{$key};
                        my $new_match = $oid;
                        $new_match =~ s/$var/$val/;
                        $lut{$new_match} = $key;
                    }

                    if(defined $type && $type eq "rate"){
			$self->logger->error("Asking SIMP for rate: " . Dumper(\@hostarray) . " for Match: " . $match);
			$self->client->get_rate(
                            node => \@hostarray,
			    period => $params->{'period'}{'value'},
                            oidmatch => $match,
			    async_callback =>  sub {
				my $data= shift;
				$self->_val_cb($data->{'results'},$results,$host,$id,\%lut,$val);
				$cv->end;
			    } );

		    }else{
			$self->logger->error("Asking SIMP for: " . Dumper(\@hostarray) . " for Match: " . $match);
			$self->client->get(
                            node => \@hostarray,
                            oidmatch => $match,
			    async_callback =>  sub {
				my $data= shift;
				$self->_val_cb($data->{'results'},$results,$host,$id,\%lut,$val);
				$cv->end;
			    } );
		    }
		}
	    } 
	}
    }
    $cv->end; 
}

sub _hostvar_cb{
}

sub _val_cb{
  my $self        = shift;
  my $data        = shift;
  my $results     = shift;
  my $hosts       = shift;
  my $id          = shift;
  my $lut         = shift;
  my $xref        = shift;

  my $doc = $self->config->{'doc'};
  my $xc  = XML::LibXML::XPathContext->new($doc);

  my %groups;
  foreach my $host (keys %$data){    
    foreach my $oid (keys %{$data->{$host}}){
	my $val = $data->{$host}{$oid}{'value'};
		
	my $var = $lut->{$oid};
	if(!defined($var)){
	    #well shoot its not a direct match... so there is the possiblity we have additional datas!
	    foreach my $key (keys (%{$lut})){
		if($oid =~ /$key\./){
		    #we found it!
		    $var = $lut->{$key};
		}
	    }
	}

	if(!defined($var)){
	    next;
	}

	if(!defined($results->{'final'}{$host}{$var}{'time'})){
	    $results->{'final'}{$host}{$var}{'time'} = $data->{$host}{$oid}{'time'};
	}

	if(!defined($groups{$var})){
	    $groups{$var} = ();
	}

	push(@{$groups{$var}}, $val);

    }

    foreach my $group (keys (%groups)){
	$self->_do_functions_old(values => $groups{$group},
			     xpath => $xref,
			     results => $results->{'final'}{$host}{$group},
			     id => $id);
    }
    
  }
  return;
}


sub _do_functions{
  my $self         = shift;
  my $xrefs        = shift;
  my $params       = shift;
  my $results      = shift;
  my $cv           = shift; # assumes that it's been begin()'ed with a callback

  $cv->end;
}

sub _do_functions_old{
    my $self = shift;
    my %params = @_;

    my $vals = $params{'values'};
    my $var = $params{'var'};
    my $xref = $params{'xpath'};
    my $results = $params{'results'};
    my $id = $params{'id'};

    my $doc = $self->config->{'doc'};
    my $xc  = XML::LibXML::XPathContext->new($doc);
    
    my $fctns = $xc->find("./fctn",$xref);
    foreach my $fctn ($fctns->get_nodelist){
	my $name      = $fctn->getAttribute("name");
	my $operand     = $fctn->getAttribute("value");
	
	if($name eq "max" || $name eq "min" || $name eq "sum"){

	    my $new_val;
	    if($name eq 'sum'){
		$new_val = 0;
		foreach my $val (@$vals){
		    $new_val += $val;
		}
	    }elsif($name eq 'min'){
		foreach my $val (@$vals){
		    if(!defined($new_val)){
			$new_val = $val;
		    }
		    if($new_val > $val){
			$new_val = $val;
		    }
		}
	    }elsif($name eq 'max'){
		foreach my $val (@$vals){
		    if(!defined($new_val)){
                        $new_val = $val;
                    }
                    if($new_val > $val){
                        $new_val = $val;
                    }
		}
	    }else{
		$self->logger->error("Unknown consolidation function: $name");
	    }
	    $vals = [$new_val];
	}else{

	    foreach my $val (@$vals){

                next if !defined($val); # assume we should have that `undef <op> anything == undef`
		
		if($name eq "/"){
		    #not supported in ARRAY FORM
		    #--- unary divide operator
		    $val = $val / $operand;
		}elsif($name eq "*"){
		    #--- unary multiply operator
		    $val = $val * $operand;
		}elsif($name eq "+"){
		    #--- unary addition operator
		    $val = $val + $operand;
		}elsif($name eq "-"){
		    #--- unary subtraction operator
		    $val = $val - $operand;
		}elsif($name eq "%"){
		    #--- unary modulo operator
		    $val = $val % $operand;
		}elsif($name eq "ln"){
		    #--- base-e logarithm
                    $val = eval { log($val); }; # if val==0, we want the result to be undef, so this works just fine
		}elsif($name eq "log10"){
		    #--- base-10 logarithm
                    $val = eval { log($val); }; # see ln
                    $val /= log(10) if defined($val);
		}elsif($name eq "regexp"){
		    $val =~ /$operand/;
		    $val = $1;
		}elsif($name eq "replace"){
		    my $replace_with = $fctn->getAttribute("with");
		    $operand =~ s/$var/$val/;
		    $replace_with =~ s/$var/$val/;
		    $val =~ s/$operand/$replace_with/;
		}else{
		    $self->logger->error("Unknown function: $name");
		}
	    }
	}
    }

    $results->{$id} = $vals->[0];

}

1;
