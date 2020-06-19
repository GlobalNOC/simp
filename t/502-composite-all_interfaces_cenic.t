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
my $composite = 'all_interfaces_cenic';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

my $tot_in      = 0;
my $tot_out     = 0;
my $tot_ifalias = 0;
for my $entry (@{$data->{$node}}) {
    $tot_in  += $entry->{'input'};
    $tot_out += $entry->{'output'};
    $tot_ifalias++ if (exists $entry->{'ifAlias'});
}

ok(scalar(keys $data) == 1,         "Responds with correct number of node results");
ok($data->{$node},                  "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 2,  "Responds with correct number of data results");
ok($tot_in == 40 && $tot_out == 40, "Correct input and output values calculated");
ok(!$tot_ifalias,                   "Correctly drops ifAlias values");

