#!/usr/bin/perl

use strict;
use warnings;

use FindBin;

use Test::More tests => 1;

my $status = system("$FindBin::Bin/integration-start.sh", 'arg');

ok($status == 0, 'services successfully started up for integration tests');
