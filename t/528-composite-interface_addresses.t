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
my $composite = 'interface_addresses';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

my $has_value = 0;
my $used_sfx  = 0;
# Check data returned for changes made in conversions
for my $entry (@{$data->{$node}}) {
    $has_value++ if (defined $entry->{'unused'});
    $used_sfx += $entry->{'*intf_index'};
}

ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 2, "Responds with correct number of data results");
ok($has_value == 2,                "Contstant value for 'unused' applied to metadata-only collection");
ok($used_sfx == 3,                 "Correctly uses oid suffix as values for 'intf_index'");
