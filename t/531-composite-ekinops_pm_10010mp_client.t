#!/usr/bin/perl


use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_deeply num any code);
use Test::More tests => 7;

use Test::MockModule;
use Test::MockObject;
use FindBin;

use lib "$FindBin::Bin/lib";
use SimpTesting;

# Define the composite to test and the node name in the test dataset
my $composite = 'ekinops_pm_10010mp_client';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

# Run comp for a node being excluded by conversions
my $ex_data = $test->comp_get('excluded_node.grnoc.iu.edu', $composite);
#warn Dumper($ex_data);

# Ensure the excluded node has no node data set
my $node_exclusions = 4;
for my $entry (@{$ex_data->{'excluded_node.grnoc.iu.edu'}}) {
    $node_exclusions-- if (defined $entry->{'*node'});
}

my $correct_opt = 0;
my $valid_intf  = 0;
my $replaced    = 0;
for my $entry (@{$data->{$node}}) {
    $correct_opt++ if ($entry->{'rxpower'} == -30 && $entry->{'txpower'} == -30);
    if (defined $entry->{'*intf'}) {
        $valid_intf++;
        $replaced++ if ($entry->{'*intf'} =~ m/^1\/.*$/);
    }
}

ok(scalar(keys $data) == 1,         "Responds with correct number of node results");
ok($data->{$node},                  "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 4,  "Responds with correct number of data results");
ok($node_exclusions == 4,           "Excludes nodes with underscores");
ok($correct_opt == 4,               "Correct rx/tx power values calculated");
ok($valid_intf == 2,                "Correctly matches desired interface names");
ok($replaced == 2,                  "Correctly appends \"1/\" to valid interface names");

