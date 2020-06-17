#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_deeply num any code);
use Test::More tests => 3;

use Test::MockModule;
use Test::MockObject;
use FindBin;

use lib "$FindBin::Bin/lib";
use SimpTesting;

use constant THRESHOLD => 1e-9;

my $testing = SimpTesting->new(data_set_name => "all_interfaces");

my $data = $testing->comp_get("rtsw.chic", "all_interfaces");

is(keys $data, 1, "got 1 node result back");
ok($data->{'rtsw.chic'}, "got a response for requested node");

is(@{$data->{'rtsw.chic'}}, 4, "got 4 interfaces back");
