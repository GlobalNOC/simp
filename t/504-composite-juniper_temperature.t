#!/usr/bin/perl


use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_deeply num any code);
use Test::More tests => 6;

use Test::MockModule;
use Test::MockObject;
use FindBin;

use lib "$FindBin::Bin/lib";
use SimpTesting;

# Define the composite to test and the node name in the test dataset
my $composite = 'juniper_temperature';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);

# Get the number of entries with appropriately matching names
my $name_count   = 0;
my $temp_count   = 0;
my $removed_fan1 = 0;

# Check data returned for changes made in conversions
for my $entry (@{$data->{$node}}) {
    $name_count++ if (defined $entry->{'*name'});
    $temp_count++ if (defined $entry->{'temp'});
    $removed_fan1++ if ($entry->{'*name'} eq 'FPC');
}

ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 6, "Responds with correct number of data results");
ok($name_count == 3,               "Regex match for *name setting undef for non-matches");
ok($temp_count == 5,               "Excluding temperature readings of 0 degrees");
ok($removed_fan1,                  "Removed 'Fan 1' from valid *name value");

