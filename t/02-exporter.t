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

# This flag will cause the test to run better for benchmarking
my $benchmarking = 0;

my $exporter = GRNOC::Simp::Exporter->new(
    config_file     => '/etc/simp/exporter/config.xml',
    logging_file    => '/etc/simp/exporter/logging.conf',
    validation_file => '/etc/simp/exporter/validation.d/config.xsd'
);

$exporter->export("TSDS", "critical", "push", "Failed to push tsds message to the tsds service");

ok(1);
done_testing(1);
