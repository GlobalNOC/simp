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
my $composite = 'netguardian_fuel';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

# Get the number of entries with appropriately matching names
my $name_count   = 0;
my $fuel_count   = 0;

# Check data returned for changes made in conversions
for my $entry (@{$data->{$node}}) {
    $name_count++ if (defined $entry->{'*name'});
    $fuel_count++ if (defined $entry->{'fuel'});
}

ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 4, "Responds with correct number of data results");
ok($name_count == 3,               "Regex match for *name setting undef for non-matches");
ok($fuel_count == 3,               "Excluding invalid readings for 'fuel'");

