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
my $composite = 'hp_temperature';
my $node      = 'acme.grnoc.iu.edu';

# Define new test for composite using JSON data in data_sets of the same name
my $test = SimpTesting->new(data_set_name => $composite);

# Run comp for the composite and node
my $data = $test->comp_get($node, $composite);
#warn Dumper($data);

#my $value = @{$data->{$node}}[0];
my $tot_temp = 0;
my $sfx_name = 0;
for my $d (@{$data->{$node}}) {
    $tot_temp++ if ($d->{'temp'});
    $sfx_name++ if ($d->{'*name'} eq 1 || $d->{'*name'} eq 2); 
}


ok(scalar(keys $data) == 1,        "Responds with correct number of node results");
ok($data->{$node},                 "Responds with data for the requested node");
ok(scalar(@{$data->{$node}}) == 2, "Responds with correct number of data results");
ok($sfx_name == 2,                 "Value for 'name' from oid_suffix is correct");
ok($tot_temp == 1,                 "Drops temperature values that are zero");
