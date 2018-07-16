#!/usr/bin/perl -w
use strict;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use GRNOC::WebService;
use JSON;
use Data::Dumper;
use Redis;
#use Test::More tests => 1;
# Reading the config file
my $config      = GRNOC::Config->new(
			config_file     => "/etc/simp/redis_config.xml",
			debug => 0,
			force_array => 0
				);
my $info = $config->get("/config");
#ok(defined($config), "Config<<<");

# Setting up Redis
my $redis_host  = $info->{'redisInfo'}{'host'};
my $redis_port  = $info->{'redisInfo'}{'port'};


warn Dumper("$redis_host $redis_port");
my $redis=Redis->new(server => $redis_host.":".$redis_port);



sub get_groups{
	
	my $method_obj	= shift;
	my $params	= shift;
	my %results;
	my $ip	= $params->{'ip'}{'value'};
	my %gids; #Creating a hash map for storing only unique group_names 
	my @output;
	$redis->select(3);


	my %arr	= $redis->hgetall($ip);
	
	#Get Group Name, interval from redis
	while (my($key,$value)	= each(%arr))
	{
		my($group_name,$interval)	= split(",",$value);
		$gids{$group_name}		= 1;
	}
	while(my $temp = each(%gids))
	{
		push @output, $temp;
	}
	$results{'groups'} = \@output;;
	return \%results;

}

sub get_timestamp{

        my $mthod_obj = shift;
        my $params    = shift;

        my $ip=$params->{'ip'}{'value'};

        $redis->select(3);
        my %arr=$redis->hgetall($ip);
        my $key;
        my $value;
        my %gids;
        my %poll_ids;
        my %results;
        my $count=1;
        my %dict;
	my $hostname; # Get the hostname from ip using db 1
        while (($key,$value) = each (%arr))
        {
                my ($gid,$interval)=split(',', $value);
                $gids{$gid}=1;

        }
        my $group_name;
        my %db1_op;
        while(($group_name,$value) = each (%gids))
        {
                $redis->select(2);
                my $temp = $redis->get($ip.",".$group_name);
                my ($pid,$timestamp)=split(',',$temp);
                my @ts = ($pid , $timestamp);
		
                $dict{$group_name}=\@ts;
                $redis->select(1);
	        my ($node_name,$group,$number,$time)=split(",",$redis->get($ip.",".$group_name.",".$pid));
		$db1_op{$group_name}{'node'}	= $node_name;
		$db1_op{$group_name}{'group'}	= $group;
		$db1_op{$group_name}{'wid'}	= $number;
		$db1_op{$group_name}{'time'}	= $time;
		$hostname			= $node_name;
	}
	$redis->select(0);
	# Get all keys in DB0 that contain hostname
        my %keys = $redis->keys("*$hostname*");
	my %timestamp;
	while( my ($key) = each (%keys))
	{
		my ($local_ip,$worker_name,$timestamp)	= split(",",$key);
		my $group			= $worker_name;
		$worker_name			=~ s/[^0-9]//g;
		my $worker_id			=~ s/[^0-9]//g;;
		$group				=~ s/$worker_name//g;
		$timestamp{$key}{'ip'}		= $local_ip;
		$timestamp{$key}{'group'}	= $group;		
		$timestamp{$key}{'wid'}		= $worker_name;
		$timestamp{$key}{'timestamp'}	= $timestamp;

	}	
        my $json = encode_json \%dict;
        $results{'groups'}= \%dict;
        $results{'key0'}=\%db1_op;
	$results{'timestamps'}=\%timestamp;
        return \%results;
}


sub get_data{
        my $method_obj  = shift;
        my $params      = shift;
        my $ip          =       $params->{'ip'}{'value'};
        my $group_id    =       $params->{'group_name'}{'value'};
        my $worker_id   =       $params->{'worker_id'}{'value'};
        my $timestamp   =       $params->{'timestamp'}{'value'};

        $redis->select(0);
        my @data        =       $redis->smembers($ip.",".$group_id.$worker_id.','.$timestamp);
        my %dict;
        foreach (@data)
        {
                my $oid =substr( $_,0, index($_ , ","));
                my $oid_data = substr($_, index($_ , ",")+1,-1);
                $dict{$oid}= $oid_data;
        }
        my %result;
        $result{'oid'}=\%dict;
        return \%result;

}


my $get_groups_method	= GRNOC::WebService::Method->new(
							name		=> 'get_groups',
							description	=> 'Get groups given the ip address',
							callback	=> \&get_groups
						);

$get_groups_method->add_input_parameter(
						name		=>'ip',
						pattern		=> '((\d*\D*)*)$',
						required	=> 1,
						description	=> 'ip-address'				
					);


my $svc	= GRNOC::WebService::Dispatcher->new();

my $res	= $svc->register_method($get_groups_method);

my $get_timestamp =GRNOC::WebService::Method->new(
						name		=> "get_timestamp",
						description	=> "fetches timestamp given the ip and group id",
						callback	=> \&get_timestamp
					);

$get_timestamp->add_input_parameter(
						name		=> "ip",
						pattern		=> '((\d*\D*)*)$',
						required	=> 1,
						description	=> 'ip address'
				);

#$get_timestamp->add_input_parameter(
#						name		=> 'group',
#						pattern		=> '((\d*\D*)*)$',
#						required	=> 1,
#						description	=> 'group_name'
#				);

my $res2 = $svc->register_method($get_timestamp);

my $get_data_method =GRNOC::WebService::Method->new(
                                                name            => "get_data",
                                                description     => "descr",
                                                callback        => \&get_data
                                                );
$get_data_method->add_input_parameter(
                                name            => 'ip',
                                pattern         => '((\d*\D*)*)$',
                                required        => 1,
                                description     => "ip-address"
                        );
$get_data_method->add_input_parameter(
                                name            => 'group_name',
                                pattern         => '((\d*\D*)*)$',
                                required        => 1,
                                description     => "group name"
                        );

$get_data_method->add_input_parameter(
                                name            => 'worker_id',
                                pattern         => '((\d*\D*)*)$',
                                required        => 1,
                                description     => "worker id"
                        );

$get_data_method->add_input_parameter(
                                name            => 'timestamp',
                                pattern         => '((\d*\D*)*)$',
                                required        => 1,
                                description     => "timestamp"
                        );
my $res3 = $svc->register_method($get_data_method);

my $res_end  = $svc->handle_request();

