#!/usr/bin/perl

#use GRNOC::Simp::Data::Worker;

use strict;
use warnings;

use FindBin;                   # locate this script
use lib "$FindBin::Bin/..";    # use the parent directory

use Data::Dumper;
use Test::More;

use GRNOC::Config;
use GRNOC::Log;
use Worker;

# Reading the config file
my $config = GRNOC::Config->new(
    config_file => "/etc/grnoc/webservice_client/redis_config.xml",
    debug       => 0,
    force_array => 0
);

my $info = $config->get("/config");

ok(defined($info), "config file is initialized");

# Setting logging file
my $grnoc_log =
  GRNOC::Log->new(config => "/etc/grnoc/logs/log_unit_test.conf", watch => 120);
my $logger = GRNOC::Log->get_logger();

print "1..1\n";

ok(defined($logger), "logger<<<");
ok(defined($config), "<<<<<<config");

#setting Redis

my $redis_host = $info->{'redis'}{'host'};
my $redis_port = $info->{'redis'}{'port'};
warn Dumper($redis_host . $redis_port);
my $redis = Redis->new(server => $redis_host . ":" . $redis_port);

my $object = GRNOC::Simp::Data::Worker->new(
    config    => $config,
    logger    => $logger,
    worker_id => 13,
    redis     => $redis
);

ok(defined($object), "Object is defined");
my $response = $object->_ping();
ok(defined($response), "got a response as $response");

my $response = $object->_is_oid_prefix($info->{'object'}{'prefix'},
    $info->{'object'}{'suffix'});
ok(defined($response), "<< Testing is_ois_prefix. Response=> $response");

my %input = (
    host      => $info->{'testFindKeys'}{'host'},
    oid       => $info->{'testFindKeys'}{'oid'},
    requested => $info->{'testFindKeys'}{'requested'}
);

my $response = $object->_find_keys(%input);
ok(defined($response), "<<_find_groups ->" . \$response);

my $response = $object->_find_groups(%input);
ok(defined($response), ">>_find_froups ->" . \$response);

# Prep for _get

my $requested = $info->{'testGet'}{'requested'};

my $params = {
    node => {
        value => [$info->{'testGet'}{'node'}]
    },
    oidmatch => {
        value => [$info->{'testGet'}{'oid'}]
    }
};

my $result = $object->_get($requested, $params);
ok(defined($result), "<<< testing _get -> " . $result);

# Test _rate
my $cur_val = $info->{'testRate'}{'cur_val'};
my $cur_ts  = $info->{'testRate'}{'cur_ts'};
my $pre_val = $info->{'testRate'}{'pre_val'};
my $pre_ts  = $info->{'testRate'}{'pre_ts'};
my $context = $info->{'testRate'}{'context'};

my $response = $object->_rate($cur_val, $cur_ts, $pre_val, $pre_ts, $context);
ok(defined($response), "<<<Testing _rate" . $response);

# Test _get_rate
my %response = $object->_get_rate($params);
ok(defined(%response), "<<< Testing _get_rate" . %response);

done_testing();
