#!/usr/bin/perl
use strict;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use GRNOC::WebService;
use JSON;
use Data::Dumper;
use GRNOC::RabbitMQ::Client;
use AnyEvent;
use GRNOC::Config;

# warn "----- ENTRY POINT -----";
#------ Variables
my $method_obj;
my $params;
my $host;
my $oid;
my $svc;
my $config_file = "/etc/simp/compDataConfig.xml";
my $config = GRNOC::Config->new(config_file => $config_file, force_array => 0, debug => 0); 
my $rabbit_config = $config->get("/config/rabbitMQ");

my $client = GRNOC::RabbitMQ::Client->new(   host => $rabbit_config->{'host'},
# my $client = GRNOC::RabbitMQ::Client->new(   host => "io3.bldc.grnoc.iu.edu",
    port => $rabbit_config->{'port'},
    user => $rabbit_config->{'user'},
    pass => $rabbit_config->{'password'},
    exchange => 'Simp',
    timeout => 60,
    topic => 'Simp.CompData');




#------ callback method
sub _get{

    my $self= shift;
    $params = shift;
    # $method_obj = $params->{'method'}{'value'};
    # $host = $params->{'host'}{'value'};
    my $results;
    $results = $client->$method_obj(
        node     => [$host],
        period   => 60 
    );
    
    return $results;
}
#------ create methods 
sub register_methods {

    my $allowed_methods = $config->get("/config/composite");
    foreach my $meth (@$allowed_methods){
        my $method_id = $meth->{'id'};
        # warn Dumper($method_id);
        my $method = GRNOC::WebService::Method->new(  name => "$method_id",
            callback =>  sub {_get(@_) },
            description => "retrieve composite simp data of type $method_id, we should add a descr to the config");

        $method->add_input_parameter( name => 'host',
            description => 'nodes to retrieve data for',
            pattern => '^(.*)$'); 

        $method->add_input_parameter( name => 'method',
            description => 'method for comp',
            pattern => '^(.*)$'); 

        $svc->register_method($method);
    }

}

#------ Dispatcher
$svc = GRNOC::WebService::Dispatcher->new();

#------
register_methods();

#------ go to town
$svc->handle_request();


