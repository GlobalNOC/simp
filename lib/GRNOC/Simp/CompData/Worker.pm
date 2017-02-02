package GRNOC::Simp::CompData::Worker;

use strict;
use Carp;
use Time::HiRes qw(gettimeofday tv_interval);
use Data::Dumper;
use Try::Tiny;
use Moo;
use Redis;
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

    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new( 	queue => "Simp.CompData",
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


      #--- let xpath do the iteration for us
      my $path = "/config/composite[\@id=\"$method_id\"]/input";
      my $inputs = $self->config->get($path);
      foreach my $input (@$inputs){
        my $input_id = $input->{'id'};
        my $required = 0;
        if(defined $input->{'required'}){$required = 1;}

        $method->add_input_parameter( name => "$input_id",
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

sub _do_scans{
  my $self         = shift;
  my $xrefs        = shift;
  my $params       = shift;
  my $results      = shift;
  my $onComplete   = shift;


  #--- find the set of required variables
  #-- for now hack host as its sorta special
  my $hosts = $params->{'node'}{'value'};
  

  #--- this function will execute multiple scans in "parallel" using the begin / end apprach
  #--- this first call to begin will call the $onComplete function when the number of end calls == number of begin
  my $cv = AnyEvent->condvar;
  $cv->begin($onComplete);
  
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

  $oid_pattern  =~s/\*//;
  $oid_pattern = quotemeta($oid_pattern);

  foreach my $host (@$hosts){
      foreach my $oid (keys %{$data->{$host}}){
	  if(defined $vals){
	      #--- return only those entries matching specified values
	      foreach my $val (@$vals){
		  if($data->{$host}{$oid}{'value'} eq $val){
		      $oid =~ s/$oid_pattern//;
		      $results->{$host}{$id}{$val} = $oid;
		  }
	      }
	  }else{
	      #--- no val specified, return all
	      my $val = $data->{$host}{$oid}{'value'};
	      $oid =~ s/$oid_pattern//;
	      $results->{$host}{$id}{$val} = $oid;
	  }
      }
  }
  
  return ;
}

sub _do_vals{
    my $self         = shift;
    my $xrefs        = shift;
    my $params       = shift;
    my $results      = shift;
    my $onComplete   = shift;
    
    #--- find the set of required variables
    #-- for now hack host as its sorta special
    my $hosts = $params->{'node'}{'value'};
    
    
    #--- this function will execute multiple gets in "parallel" using the begin / end apprach
    #--- this first call to begin will call the $onComplete function when the number of end calls == number of begin
    my $cv = AnyEvent->condvar;
    $cv->begin($onComplete);
    
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
		
		next;
	    }
	    
	    if(!defined $oid){
		
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
		foreach my $key (keys %{$ref}){
		    my $val = $ref->{$key};
		    my $match = $oid;
		    $match =~ s/$var/$val/;
		    $lut{$match} = $key;
		    push(@matches,$match);
		}
		push(@hostarray,$host);
		#--- send the array of matches to simp
		$cv->begin;
		
		if(defined $type && $type eq "rate"){
		    $self->client->get_rate(
                        ipaddrs => \@hostarray,
                        oidmatch => \@matches,
                        async_callback =>  sub {
			    my $data= shift;
			    $self->_val_cb($data->{'results'},$results,$host,$id,\%lut,$val);
			    $cv->end;
			} );
		    
		    
		}else{
		    $self->client->get(
			ipaddrs => \@hostarray, 
			oidmatch => \@matches,
			async_callback =>  sub {
			    my $data= shift; 
			    $self->_val_cb($data->{'results'},$results,$host,$id,\%lut,$val); 
			    $cv->end;
			} );      
		    
		}
	    }
	} 
    }
    $cv->end; 
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

  foreach my $host (keys %$data){
    foreach my $oid (keys %{$data->{$host}}){
      my $val = $data->{$host}{$oid}{'value'};
      #--- use lookup table to map the OID back to a human readable value
      my $var = $lut->{$oid};

      my $fctns = $xc->find("./fctn",$xref);
      foreach my $fctn ($fctns->get_nodelist){
        my $name      = $fctn->getAttribute("name");
        my $operand     = $fctn->getAttribute("value");
       
        if($name eq "/"){
	  #--- unary divide operator
	  $val = $val / $operand
	}
        if($name eq "*"){
          #--- unary multiply operator
          $val = $val * $operand
        }
        if($name eq "regexp"){
	    $val =~ /$operand/;
	    $val = $1;
        }
      }
      $results->{'final'}{$host}{$var}{$id} =  $val; #sprintf("%.4f", $val);
    }
  } 

   return;
}


sub _get{
  my $self      = shift;
  my $composite = shift;
  my $rpc_ref   = shift;
  my $params    = shift;

  my %results;  

  #--- figure out hostType
  my $hostType = "default";

  #--- give up on config object and go direct to xmllib to get proper xpath support
  my $doc = $self->config->{'doc'};
  my $xc  = XML::LibXML::XPathContext->new($doc);

  #--- get the instance
  my $path = "/config/composite[\@id=\"$composite\"]/instance[\@hostType=\"$hostType\"]";
  my $ref = $xc->find($path);


  #--- because we have to do things asyncronously, execution from here follows a nested set of callbacks basically
  #--- _do_scans -> _do_vals -> success
  #---   \->_scan_cb    \->_val_cb 
  #--- results are accumulated in $results{'final'} 
  my $success_callback = $rpc_ref->{'success_callback'};


  my $onSuccess = sub { my $cv = shift;

			\&$success_callback($results{'final'});
  };
  $self->_do_scans(
      $ref,
      $params,
      \%results,
      sub {
	  $self->_do_vals($ref,$params,\%results,$onSuccess);
      });
  
}

1;
