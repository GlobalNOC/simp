#!/usr/bin/perl


use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_deeply num any code);
use Test::More tests => 4;

use Test::MockModule;
use Test::MockObject;
use FindBin;

use lib "$FindBin::Bin/lib";
use SimpTesting;

# Define the composite to test and the node name in the test dataset
my $composite = 'netguardian_alarm';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

# Get the number of entries with appropriately matching names
my $alarm_count = 0;

# Check data returned for changes made in conversions
for my $entry (@{$data->{$node}}) {
    $alarm_count++ if ($entry->{'status'} eq 1);
}

ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 4, "Responds with correct number of data results");
ok($alarm_count == 2,              "Correctly sets 0/1 for Clear/Alarm status");

