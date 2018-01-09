#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 1;

# GRNOC::Simp::CompData::_rpn_calc($val, $progtext, $fctn_elem[unused], $val_set, $results, $host_name)

use GRNOC::Simp::CompData::Worker;

sub rpn_calc {
  return GRNOC::Simp::CompData::Worker::_rpn_calc(@_);
}

# Result of using computation
my $res;

# Empty program returns passed-in value
$res = rpn_calc('hello world', '', undef, {}, {}, 'example.org');
ok($res eq 'hello world', 'empty program returns passed-in value');
