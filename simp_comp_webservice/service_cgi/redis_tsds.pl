#!/usr/bin/perl -w
#print "Content-type: text/html\r\n\r\n";

use Redis;
use strict;
use GRNOC::WebService;
use JSON;
use Data::Dumper;
require HTTP::Request;
use GRNOC::WebService::Client;
use GRNOC::Config;

# Setting up REDIS
my $redis_host	= "io3.bldc.grnoc.iu.edu";
my $redis_port	= "6380";
my $redis=Redis->new(server => $redis_host.":".$redis_port);

# Reading the config file
my $config	= GRNOC::Config->new(
				config_file	=> "/var/www/cgi-bin/service_cgi/config.xml",
				debug => 0,
				force_array => 0
				);
my $info = $config->get("/config");
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
	$meta{'database'}	= "db0";
	$meta{'host'}		= $redis_host;
	my %output;
	$output{'interval'}	= 300;
	$output{'time'}		= time();
	$output{'type'}		= "simp_redis_counts";
	$output{'meta'}		= \%meta;
	$output{'values'}	= \%results;

	# Hash to JSON
	my $data	= encode_json \%output;	
	$data= '['.$data.']';
	my $url		= 'https://io3.bldc.grnoc.iu.edu/tsds/services/push.cgi';
	
	# Calling Tsds/puch.cgi
	my $svc = GRNOC::WebService::Client->new(
					url	=> $url,
					uid	=> $USERNAME,
					passwd	=> $PASSWORD,
 					usePost	=> 1
						);
	my $result = $svc->add_data(data => $data);
}

push_data()
