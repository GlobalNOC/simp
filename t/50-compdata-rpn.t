#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 18;

# GRNOC::Simp::CompData::_rpn_calc($val, $progtext, $fctn_elem[unused], $val_set, $results, $host_name)

use GRNOC::Simp::CompData::Worker;

sub rpn_calc {
  return GRNOC::Simp::CompData::Worker::_rpn_calc(@_);
}

# Result of using computation
my $res;
my @res;

# Empty program returns passed-in value
$res = rpn_calc('hello world', '', undef, {}, {}, 'example.org');
ok($res eq 'hello world', 'empty program returns passed-in value');

# Only item on top of stack is returned
@res = rpn_calc(5, '3', undef, {}, {}, 'example.org');
ok((scalar(@res) == 1) && ($res[0] == 3), 'only item on top of stack is returned');

# Whitespace in program doesn't interfere with program
$res = rpn_calc(5, '   3   ', undef, {}, {}, 'example.org');
ok($res == 3, "whitespace in program doesn't interfere with program");

# Negative numbers can be pushed
$res = rpn_calc(5, '-27', undef, {}, {}, 'example.org');
ok($res == -27 , 'negative numbers can be pushed');

# Numbers with decimal points can be pushed
$res = rpn_calc(5, '  2.375 ', undef, {}, {}, 'example.org');
ok($res == 2.375, 'numbers with decimal points can be pushed');

# String literals can be pushed
$res = rpn_calc(5, '"a string"', undef, {}, {}, 'example.org');
ok($res eq 'a string', 'strings can be pushed');

# Single-quoted strings can be pushed
$res = rpn_calc(5, "'another string'", undef, {}, {}, 'example.org');
ok($res eq 'another string', 'single-quoted strings can be pushed');

# Whitespace in program doesn't interfere with string literals
$res = rpn_calc(5, '  "xyzzy"  ', undef, {}, {}, 'example.org');
ok($res eq 'xyzzy', "whitespace in program doesn't interfere with string literals");

# Backslash escapes work in double-quoted strings
$res = rpn_calc(5, ' "ab\\"cde\\\'fgh\\\\ijk\\lmn\\\\\\o\\p" ', undef, {}, {}, 'example.org');
ok($res eq 'ab"cde\'fgh\\ijklmn\\op', 'backslash escapes work in double-quoted strings');

# Backslash escapes work in single-quoted strings
$res = rpn_calc(5, " 'ab\\\"cde\\'fgh\\\\ijk\\lmn\\\\\\o\\p'  ", undef, {}, {}, 'example.org');
ok($res eq 'ab"cde\'fgh\\ijklmn\\op', 'backslash escapes work in single-quoted strings');

# at 10 tests

# Unterminated double-quoted string literals are handled (case 1)
$res = rpn_calc(5, '  "ab\\cde ', undef, {}, {}, 'example.org');
ok($res eq 'abcde ', 'unterminated double-quoted string literals are handled (case 1)');

# Unterminated double-quoted string literals are handled (case 2)
$res = rpn_calc(5, ' "ab\\"cde\\\\', undef, {}, {}, 'example.org');
ok($res eq 'ab"cde\\', 'unterminated double-quoted string literals are handled (case 2)');

# Unterminated double-quoted string literals are handled (case 3)
$res = rpn_calc(5, ' "ab\\"cde\\"', undef, {}, {}, 'example.org');
ok($res eq 'ab"cde"', 'unterminated double-quoted string literals are handled (case 3)');

# Unterminated double-quoted string literals are handled (case 4)
$res = rpn_calc(5, ' "abcde\\', undef, {}, {}, 'example.org');
ok($res eq 'abcde', 'unterminated double-quoted string literals are handled (case 4)');

# Unterminated single-quoted string literals are handled (case 1)
$res = rpn_calc(5, "  'ab\\cde ", undef, {}, {}, 'example.org');
ok($res eq 'abcde ', 'unterminated single-quoted string literals are handled (case 1)');

# Unterminated single-quoted string literals are handled (case 2)
$res = rpn_calc(5, " 'ab\\'cde\\\\", undef, {}, {}, 'example.org');
ok($res eq 'ab\'cde\\', 'unterminated single-quoted string literals are handled (case 2)');

# Unterminated single-quoted string literals are handled (case 3)
$res = rpn_calc(5, " 'ab\\'cde\\'", undef, {}, {}, 'example.org');
ok($res eq 'ab\'cde\'', 'unterminated single-quoted string literals are handled (case 3)');

# Unterminated single-quoted string literals are handled (case 4)
$res = rpn_calc(5, " 'abcde\\", undef, {}, {}, 'example.org');
ok($res eq 'abcde', 'unterminated single-quoted string literals are handled (case 4)');

# at 18 tests

my $xxx = "
#
$res = rpn_calc(, '', undef, {}, {}, 'example.org');
ok(, '');
";
