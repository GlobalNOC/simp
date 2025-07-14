#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_details deep_diag num any bag code);
use Test::More;
use Test::MockModule;
use Test::MockObject;
use FindBin;
use lib "$FindBin::Bin/lib";
use GRNOC::Simp::Exporter;

my $logger = Log::Log4perl->get_logger('GRNOC.Simp.Comp');
# This flag will cause the test to run better for benchmarking
my $benchmarking = 0;

my $exporter = GRNOC::Simp::Exporter->new(
    config_file     => '/etc/simp/exporter/config.xml',
    logger          => $logger,
    validation_file => '/etc/simp/exporter/validation.d/config.xsd'
);

$exporter->export("TSDS", "critical", "push", "Failed to push tsds message to the tsds service");
$exporter->export("Data", "critical", "redis", "Failed to get info from redis");


$exporter->export("Data", "critical", "oid", "[_get] Unable to find keys for eugn-oh-400g-01 -> 1.3.6.1.2.1.2.2.1.14.*", '{"host": "eugn-oh-400g-01", "oid": "1.3.6.1.2.1.2.2.1.14.*"}');

$exporter->export("Comp", "critical", "redis", "Failed to get info from redis");
$exporter->export("Poller", "critical", "redis", "Failed to get info from redis");

$exporter->export("Poller", "critical", "snmp", "Failed to get info from fake.grnoc.iu.edu", '{"host": "fake.grnoc.iu.edu"}');
$exporter->export("Poller", "critical", "oid", "Failed to get info from 1.2.3.4.5:fake.grnoc.iu.edu", '{"host": "fake.grnoc.iu.edu", "oid": "1.3.6.1.2.1.2.2.1.14"}');

ok(1);
done_testing(1);
