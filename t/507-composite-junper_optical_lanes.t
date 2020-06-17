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
my $composite = 'juniper_optical_lanes';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

my $tot_rx = 0;
my $tot_tx = 0;
my $lanes  = 0;
for my $entry (@{$data->{$node}}) {
    $tot_rx += $entry->{'rxpower'};
    $tot_tx += $entry->{'txpower'};
    $lanes++ if ($entry->{'*intf'} =~ m/.*-lane\d/);
}

ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 4, "Responds with correct number of data results");
ok($tot_rx == 6 && $tot_tx == 6,   "Values for rx/tx power are correct");
ok($lanes == 4,                    "Interface names have '-lane\$num' appended");
