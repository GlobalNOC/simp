#!/usr/bin/perl

use strict;
use warnings;

use FindBin;

use Test::More tests => 1;

my $status = system("$FindBin::Bin/integration-stop.sh", 'arg');

ok($status == 0, 'all services successfully stopped after integration tests');
