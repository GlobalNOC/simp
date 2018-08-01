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



my $redis=Redis->new(server => $redis_host.":".$redis_port);





sub get_timestamp{

        my $mthod_obj = shift;
        my $params    = shift;

        my $ip=$params->{'ip'}{'value'};

        $redis->select(3);
       	my $key_count = $redis->hlen($ip);
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
		my ($node_name,$worker_name,$time)=split(",",$redis->get($ip.",".$group_name.",".$pid));
		$db1_op{$group_name}{'node'}	= $node_name;
		$worker_name                    =~ s/[^0-9]//g;
		my $worker_id                   =~ s/[^0-9]//g;
		my $group                          =~ s/$worker_name//g;
		$db1_op{$group_name}{'group'}	= $group;
		$db1_op{$group_name}{'wid'}	= $worker_name;
		$db1_op{$group_name}{'time'}	= $time;
		$hostname			= $node_name;
		
	}



	$redis->select(0);
	# Get all keys in DB0 that contain hostname
        my %keys = $redis->keys("*$hostname*");
	my $debug = $hostname;
	if ($key_count > 0 and defined($hostname)) {
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
		$results{'debug'} = $debug;
		return \%results;

		}

	$results{'debug'} = "no hostname found";
	my %keys = $redis->keys("*opt.trrh.ilight.net*");
	$results{'test'} = \%keys;
	return \%results;
}

sub get_timestamp_hostname{
        my $mthod_obj = shift;
        my $params    = shift;

        my $ip=$params->{'ip'}{'value'};
	my %results;
        $redis->select(3);
        my $key;
        my $value;
        my %gids;
        my %poll_ids;
        my %results;
        my $count=1;
        my %dict;
        my $hostname = $ip; # Get the hostname from ip using db 1i
 	$redis->select(0);	
        my %keys = $redis->keys("*$ip*");
	my $keys = %keys;
	# warn Dumper($keys);
        if (defined($hostname and $keys > 0)) {
                        my %timestamp;
                while( my ($key) = each (%keys))
                        {	
                                my ($local_ip,$worker_name,$timestamp)  = split(",",$key);
                                my $group                       = $worker_name;
				my $test			= $worker_name;
				$test				=~ s/[0-9]+$//g;
                                $timestamp{$key}{'ip'}          = $local_ip;
                                $timestamp{$key}{'group'}       = $test;
                                $timestamp{$key}{'wid'}         = $&;
                                $timestamp{$key}{'timestamp'}   = $timestamp;

                        }
                $results{'timestamps'}=\%timestamp;
                return \%results;
	}
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


my $svc	= GRNOC::WebService::Dispatcher->new();


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

my $get_timestamp_hostname =GRNOC::WebService::Method->new(
                                                name            => "get_timestamp_hostname",
                                                description     => "fetches timestamp given the hostname",
                                                callback        => \&get_timestamp_hostname
                                        );

$get_timestamp_hostname->add_input_parameter(
                                                name            => "ip",
                                                pattern         => '((\d*\D*)*)$',
                                                required        => 1,
                                                description     => 'ip address'
                                );


my $res4 = $svc->register_method($get_timestamp_hostname);

my $res_end  = $svc->handle_request();

