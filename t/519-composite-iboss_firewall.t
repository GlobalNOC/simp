#!/usr/bin/perl


use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_deeply num any code);
use Test::More tests => 5;

use Test::MockModule;
use Test::MockObject;
use FindBin;

use lib "$FindBin::Bin/lib";
use SimpTesting;

# Define the composite to test and the node name in the test dataset
my $composite = 'iboss_firewall';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

my $firewalls = 0;
my $names = 0;
for my $value (@{$data->{$node}}) {
    $firewalls++ if defined($value->{'firewall_connection'});
    $names++ if ($value->{'*name'} eq '7');
}

ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 2, "Responds with correct number of data results");
ok($names == 2,                    "Constant value for 'name' applied correctly");
ok($firewalls == 1,                "Match correctly excludes values of 'sysctl' for firewall_connection");
