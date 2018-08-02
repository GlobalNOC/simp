#!/usr/bin/perl
use strict;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use GRNOC::WebService;
use JSON;
use Data::Dumper;
use GRNOC::RabbitMQ::Client;
use AnyEvent;

#------ Variables
my $method_obj;
my $params;
my $host;
my $oid;



my $config_file = "/etc/simp/simpDataConfig.xml";
my $config = GRNOC::Config->new(config_file => $config_file, force_array => 0, debug => 0); 

my $rabbit_config = $config->get("/config/rabbitMQ");
my $client = GRNOC::RabbitMQ::Client->new(   host => $rabbit_config->{'host'},
    port => $rabbit_config->{'port'},
    user => $rabbit_config->{'user'},
    pass => $rabbit_config->{'password'},
    exchange => 'Simp',
    timeout => 60,
    topic => 'Simp.Data');


#------ SIMP: Callback
sub get {
    # if(!defined $host) {
    $method_obj = shift;
    $params = shift;
    # warn Dumper($method_obj); 
    # warn Dumper($params); 
    $host = $params->{'host'}{'value'}; 
    $oid = $params->{'oid'}{'value'};

    # }
    # warn " SIMP: Get "; 
    # warn Dumper($host);
    # warn Dumper($oid);

    my $results;
    $results = $client->get(
        node     => [$host],
        oidmatch  => [$oid],
    );
    return $results;
}


#------ SIMP: wrap callback in service method object
my $get_method = GRNOC::WebService::Method->new(

    name => "get",
    description => "descr",
    callback => \&get
);

#------ SIMP: define the parameters we will allow into this callback
$get_method->add_input_parameter (
    name => 'host',
    pattern => '^(.*)$', 
    description => "URL Parameters"
);
$get_method->add_input_parameter (
    name => 'oid',
    pattern => '^(.*)$', 
    description => "URL Parameters"
);



#------ SIMP: get_rate Callback
sub get_rate{
    my $results;
    my $period;
    # if(!defined $host) {
    $method_obj = shift;
    $params = shift;
    # warn Dumper($method_obj); 
    # warn Dumper($params); 
    $host = $params->{'host'}{'value'}; 
    $oid = $params->{'oid'}{'value'};
    $period = $params->{'period'}{'value'};
    #  }
    # warn " SIMP: Get Rate "; 
    # warn Dumper($host);
    # warn Dumper($oid);
    # warn Dumper($period);
    $results = $client->get_rate(
        node     => [$host],
        oidmatch  => [$oid],
        period => $period 
    );
    return $results;
}
#------ SIMP: get_rate wrap callback in service method object
my $get_rate_method = GRNOC::WebService::Method->new(

    name => "get_rate",
    description => "descr",
    callback => \&get_rate
);

#------ SIMP: get_rate define the parameters we will allow into this callback
$get_rate_method->add_input_parameter (
    name => 'host',
    pattern => '^(.*)$', 
    description => "URL Parameters"
);
$get_rate_method->add_input_parameter (
    name => 'oid',
    pattern => '^(.*)$', 
    description => "URL Parameters"
);

$get_rate_method->add_input_parameter (
    name => 'period',
    description => "URL Parameters"
);

#------ create dispatcher
my $svc = GRNOC::WebService::Dispatcher->new();

#------ bind our method 
my $res = $svc->register_method($get_method);
my $res1 = $svc->register_method($get_rate_method);

#------ go to town
my $res2 = $svc->handle_request();

