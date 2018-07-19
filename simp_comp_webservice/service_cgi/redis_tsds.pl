#!/usr/bin/perl -w
use Redis;
use strict;
use GRNOC::WebService;
use JSON;
use Data::Dumper;
use GRNOC::WebService::Client;
use GRNOC::Config;
use GRNOC::Log;

# Reading the config file
my $config	= GRNOC::Config->new(
				config_file	=> "/etc/simp/redis_config.xml",
				debug => 0,
				force_array => 0
				);
my $info = $config->get("/config");

# Setting logging file
my $grnoc_log	= GRNOC::Log->new( config => "/etc/grnoc/logs/log.conf", watch => 120 );
my $logger	= GRNOC::Log->get_logger();


# Setting up REDIS
my $redis_host	= $info->{'redisInfo'}{'host'};
my $redis_port	= $info->{'redisInfo'}{'port'};
my $redis=Redis->new(server => $redis_host.":".$redis_port);

# Getting credentials for webservice
my $USERNAME	= $info->{'userInfo'}{'username'};
my $PASSWORD	= $info->{'userInfo'}{'password'};

# Getting the count of objects and keys in REDIS::DB0
sub get_count{
	$redis->select(0);
        my $total_keys  = $redis->dbsize();
        my @keys        = $redis->keys("*");
        my $n = scalar @keys;
        my $total_obj   = 0.0;
        my $i=0;
        my @arr;
        my $arrSize;
        foreach(@keys){
                if (index($_, ",") != -1){
			$total_obj      += $redis->scard($_);
				}
			}

	my $mthod_obj = shift;
	my $params    = shift;
	my %results;
	$results{'key_count'}   = $total_keys;
	$results{'obj_count'}   = $total_obj;
	return %results;
}

# Pushing the data to TSDS using GRNOC::Config
sub push_data{

	# Getting count from the method above.
	my %results	= get_count();

	# Making the data frame as required by TSDS	
	my %meta;
	$meta{'database'}	= $info->{'tsdsInfo'}{'database'};
	$meta{'host'}		= $redis_host;
	my %output;
	$output{'interval'}	= $info->{'tsdsInfo'}{'interval'};;
	$output{'time'}		= time();
	$output{'type'}		= $info->{'tsdsInfo'}{'type'};;
	$output{'meta'}		= \%meta;
	$output{'values'}	= \%results;

	# Hash to JSON
	my $data	= encode_json \%output;	
	$data= '['.$data.']';
	my $url		=$info->{'tsdsInfo'}{'url'};	
	# Calling Tsds/puch.cgi
	my $svc = GRNOC::WebService::Client->new(
					url	=> $url,
					uid	=> $USERNAME,
					passwd	=> $PASSWORD,
 					usePost	=> 1
						);
	my $res = $svc->add_data(data => $data);

   	if(!defined $res){
	        log_error($svc->get_error());
	   }
	else{
	       log_info(values $res);
	   }
}

push_data()
