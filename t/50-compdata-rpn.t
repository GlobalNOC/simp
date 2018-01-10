#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 85;

# Unit tests of the following function:
#
# GRNOC::Simp::CompData::_rpn_calc($val, $progtext, $fctn_elem [unused], $val_set, $results, $host_name)
use GRNOC::Simp::CompData::Worker;

sub rpn_calc {
  return GRNOC::Simp::CompData::Worker::_rpn_calc(@_);
}

# Some sample data
my $val_set = {
    'a'   => 23,
    'abc' => 'zyx',
    '2a'  => 99,
    'a2c' => 98,
    'bb'  => 97,
    'example.org' => 'wrong',
};
my $results_test = {
  val => {
    'example.org' => {
        '12' => $val_set,
    },
  },
  hostvar => {
    'example.org' => {
        'a'   => 'XYa',
        'abc' => 'XYabc',
        '2a'  => 'XY2a',
        'a2c' => 'XYa2c',
        'bb'  => 'XYbb',
        'example.org' => 'wrong',
    },
    'otherhost.example.net' => {
        'a'   => 'wrong',
        'abc' => 'wrong',
        '2a'  => 'wrong',
        'a2c' => 'wrong',
        'bb'  => 'wrong',
        'example.org' => 'wrong',
    },
  },
};

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



# Associated-values tokens push proper value (1)
$res = rpn_calc(5, '$a', undef, $val_set, $results_test, 'example.org');
ok($res == 23, 'associated-values tokens push proper value (1)');

# Associated-values tokens push proper value (2)
$res = rpn_calc(5, ' $a ', undef, $val_set, $results_test, 'example.org');
ok($res == 23, 'associated-values tokens push proper value (2)');

# Associated-values tokens push proper value (3)
$res = rpn_calc(5, ' $abc', undef, $val_set, $results_test, 'example.org');
ok($res eq 'zyx', 'associated-values tokens push proper value (3)');

# Associated-values tokens push proper value (4)
$res = rpn_calc(5, '$2a ', undef, $val_set, $results_test, 'example.org');
ok($res == 99, 'associated-values tokens push proper value (4)');

# Associated-values tokens push proper value (5)
$res = rpn_calc(5, ' $a2c ', undef, $val_set, $results_test, 'example.org');
ok($res == 98, 'associated-values tokens push proper value (5)');

# Associated-values tokens push proper value (6)
$res = rpn_calc(5, ' $bb ', undef, $val_set, $results_test, 'example.org');
ok($res == 97, 'associated-values tokens push proper value (6)');

# Associated-values tokens push undef when value doesn't exist
$res = rpn_calc(5, ' $not_present ', undef, $val_set, $results_test, 'example.org');
ok(!defined($res), "associated-values tokens push undef when value doesn't exist");

# at 25 tests



# Host-variable tokens push proper value (1)
$res = rpn_calc(5, '#a', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYa', 'associated-values tokens push proper value (1)');

# Host-variable tokens push proper value (2)
$res = rpn_calc(5, ' #a ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYa', 'host-variable tokens push proper value (2)');

# Host-variable tokens push proper value (3)
$res = rpn_calc(5, ' #abc', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYabc', 'host-variable tokens push proper value (3)');

# Host-variable tokens push proper value (4)
$res = rpn_calc(5, '#2a ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XY2a', 'host-variable tokens push proper value (4)');

# Host-variable tokens push proper value (5)
$res = rpn_calc(5, ' #a2c ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYa2c', 'host-variable tokens push proper value (5)');

# Host-variable tokens push proper value (6)
$res = rpn_calc(5, ' #bb ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYbb', 'host-variable tokens push proper value (6)');

# Host-variable tokens push undef when value doesn't exist
$res = rpn_calc(5, ' #not_present ', undef, $val_set, $results_test, 'example.org');
ok(!defined($res), "host-variable tokens push undef when value doesn't exist");

# at 32 tests



# Hostname token pushes hostname
$res = rpn_calc(5, '@', undef, $val_set, $results_test, 'example.org');
ok($res eq 'example.org', 'hostname token pushes hostname');




# + adds two numbers
$res = rpn_calc(5, ' 7  +', undef, {}, {}, 'example.org');
ok($res == 12, '+ adds two numbers');

# + *only* adds two numbers
$res = rpn_calc(5, '7 100 +', undef, {}, {}, 'example.org');
ok($res == 107, '+ *only* adds two numbers');

# + returns undef if only one item on the stack
$res = rpn_calc(5, '+', undef, {}, {}, 'example.org');
ok(!defined($res), '+ returns undef if only one item on the stack');

# + returns undef if zero items on the stack
$res = rpn_calc(5, 'pop +', undef, {}, {}, 'example.org');
ok(!defined($res), '+ returns undef if zero items on the stack');

# at 37 tests

# - subtracts top of stack from next element down
$res = rpn_calc(10, '5 3 -', undef, {}, {}, 'example.org');
ok($res == 2, '- subtracts top of stack from next element down');

# * multiplies two numbers, and only two numbers
$res = rpn_calc(5, '37 3 *', undef, {}, {}, 'example.org');
ok($res == 111, '* multiplies two numbers, and only two numbers');

# / divides top of stack from next element down
$res = rpn_calc(19, '37 10 5 /', undef, {}, {}, 'example.org');
ok($res == 2, '/ divides top of stack from next element down');

# divide by zero yields undef
$res = rpn_calc(5, '0 /', undef, {}, {}, 'example.org');
ok(!defined($res), 'divide by zero yields undef');

# dividing zero yields a result :)
$res = rpn_calc(5, '0 5 /', undef, {}, {}, 'example.org');
ok($res == 0, 'dividing zero yields a result :)');

# at 42 tests

# + works with decimals
$res = rpn_calc(5.5, '-1.25 +', undef, {}, {}, 'example.org');
ok($res == 4.25, '+ works with decimals');

# / is not integer division
$res = rpn_calc(6, '4 /', undef, {}, {}, 'example.org');
ok($res == 1.5, '/ is not integer division');

# % takes modulus of next-to-top element divided by top element
$res = rpn_calc(35, '5 3 %', undef, {}, {}, 'example.org');
ok($res == 2, '% takes modulus of next-to-top element divided by top element');

# modulo zero yields undef
$res = rpn_calc(8, '0 %', undef, {}, {}, 'example.org');
ok(!defined($res), 'modulo zero yields undef');

# ln takes natural logarithm of top of stack (1)
$res = rpn_calc(5, '10 ln', undef, {}, {}, 'example.org');
ok(($res - 2.3026) < 1e-3, 'ln takes natural logarithm of top of stack (1)');

# ln takes natural logarithm of top of stack (2)
$res = rpn_calc(5, '1 ln', undef, {}, {}, 'example.org');
ok($res == 0, 'ln takes natural logarithm of top of stack (2)');

# natural logarithm of zero yields undef
$res = rpn_calc(5, '0 ln', undef, {}, {}, 'example.org');
ok(!defined($res), 'natural logarithm of zero yields undef');

# natural logarithm of negative number yields undef
$res = rpn_calc(-5, 'ln', undef, {}, {}, 'example.org');
ok(!defined($res), 'natural logarithm of negative number yields undef');

# at 50 tests

# log10 yields base-10 logarithm of top of stack
$res = rpn_calc(7, '100 log10', undef, {}, {}, 'example.org');
ok(($res - 2) < 1e-4, 'log10 yields base-10 logarithm of top of stack');

# log10 of zero yields undef
$res = rpn_calc(5, '0 log10', undef, {}, {}, 'example.org');
ok(!defined($res), 'log10 of zero yields undef');

# log10 of negative number yields undef
$res = rpn_calc(-5, 'log10', undef, {}, {}, 'example.org');
ok(!defined($res), 'log10 of negative number yields undef');

# exp calculates base-e exponentation (1)
$res = rpn_calc(3, '0 exp', undef, {}, {}, 'example.org');
ok($res == 1, 'exp calculates base-e exponentation (1)');

# exp calculates base-e exponentation (2)
$res = rpn_calc(3, '1 exp', undef, {}, {}, 'example.org');
ok(($res > 2.71828) && ($res < 2.71829), 'exp calculates base-e exponentation (2)');

# pow yields exponentation (1)
$res = rpn_calc(25, '2.5 3 pow', undef, {}, {}, 'example.org');
ok(abs($res - 15.625) < 1e-6, 'pow yields exponentation (1)');

# pow yields exponentation (2)
$res = rpn_calc(25, '9 -0.5 pow', undef, {}, {}, 'example.org');
ok(abs($res - 0.3333333333) < 1e-6, 'pow yields exponentation (2)');

# at 57 tests



# _ pushes undef
$res = rpn_calc(5, '6 7 8 _', undef, {}, {}, 'example.org');
ok(!defined($res), '_ pushes undef');




# defined? returns true when given a non-zero number
$res = rpn_calc(3, '4 5 _ 7 defined? ', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given a non-zero number');

# defined? returns true when given zero
$res = rpn_calc(3, '4 5 _ 0 defined?', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given zero');

# defined? returns true when given a non-empty string
$res = rpn_calc(3, '"a" "b" _ "c" defined?', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given a non-empty string');

# defined? returns true when given an empty string
$res = rpn_calc(3, '"a" "b" _ "" defined?', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given an empty string');

# defined? returns false when given undef
$res = rpn_calc(3, '"a" "b" _ defined?', undef, {}, {}, 'example.org');
ok(!$res, 'undef? returns false when given undef');

# at 63 tests



# == returns true when top two elements are numerically equal (1)
$res = rpn_calc(5, '8 8 ==', undef, {}, {}, 'example.org');
ok($res, '== returns true when top two numbers are equal (1)');

# == returns true when top two elements are numerically equal (2)
$res = rpn_calc(5, '-8 "-8" ==', undef, {}, {}, 'example.org');
ok($res, '== returns true when top two numbers are equal (2)');

# == returns false when top two elements are numerically unequal
$res = rpn_calc(5, '5 7 ==', undef, {}, {}, 'example.org');
ok(!$res, '== returns false when top two elements are numerically unequal');

# == returns true when top two elements are both undef
$res = rpn_calc(5, '_ _ ==', undef, {}, {}, 'example.org');
ok($res, '== returns true when top two elements are both undef');

# == returns false when one but not both of the top two elements are undef
$res = rpn_calc(0, '_ 0 ==', undef, {}, {}, 'example.org');
ok(!$res, '== returns false when one but not both of the top two elements are undef');

# != returns true when top two elements are numerically unequal (1)
$res = rpn_calc(5, '8 9 !=', undef, {}, {}, 'example.org');
ok($res, '!= returns true when top two numbers are unequal (1)');

# != returns true when top two elements are numerically unequal (2)
$res = rpn_calc(5, '-8 "-9" !=', undef, {}, {}, 'example.org');
ok($res, '!= returns true when top two numbers are unequal (2)');

# != returns false when top two elements are numerically equal
$res = rpn_calc(5, '7 7 !=', undef, {}, {}, 'example.org');
ok(!$res, '!= returns false when top two elements are numerically equal');

# != returns false when top two elements are both undef
$res = rpn_calc(5, '_ _ !=', undef, {}, {}, 'example.org');
ok(!$res, '!= returns false when top two elements are both undef');

# != returns true when one but not both of the top two elements are undef
$res = rpn_calc(0, '_ 0 !=', undef, {}, {}, 'example.org');
ok($res, '!= returns true when one but not both of the top two elements are undef');

# at 73 tests



# > compares the top two elements (1)
$res = rpn_calc(5, '8 3 >', undef, {}, {}, 'example.org');
ok($res, '> compares the top two elements (1)');

# > compares the top two elements (2)
$res = rpn_calc(5, '-8 3 >', undef, {}, {}, 'example.org');
ok(!$res, '> compares the top two elements (2)');

# > compares the top two elements (3)
$res = rpn_calc(5, '3 3 >', undef, {}, {}, 'example.org');
ok(!$res, '> compares the top two elements (3)');

# >= compares the top two elements (1)
$res = rpn_calc(5, '8 3 >=', undef, {}, {}, 'example.org');
ok($res, '>= compares the top two elements (1)');

# >= compares the top two elements (2)
$res = rpn_calc(5, '-8 3 >=', undef, {}, {}, 'example.org');
ok(!$res, '>= compares the top two elements (2)');

# >= compares the top two elements (3)
$res = rpn_calc(5, '3 3 >=', undef, {}, {}, 'example.org');
ok($res, '>= compares the top two elements (3)');

# <= compares the top two elements (1)
$res = rpn_calc(5, '8 3 <=', undef, {}, {}, 'example.org');
ok(!$res, '<= compares the top two elements (1)');

# <= compares the top two elements (2)
$res = rpn_calc(5, '-8 3 <=', undef, {}, {}, 'example.org');
ok($res, '<= compares the top two elements (2)');

# <= compares the top two elements (3)
$res = rpn_calc(5, '3 3 <=', undef, {}, {}, 'example.org');
ok($res, '<= compares the top two elements (3)');

# < compares the top two elements (1)
$res = rpn_calc(5, '8 3 <', undef, {}, {}, 'example.org');
ok(!$res, '< compares the top two elements (1)');

# < compares the top two elements (2)
$res = rpn_calc(5, '-8 3 <', undef, {}, {}, 'example.org');
ok($res, '< compares the top two elements (2)');

# < compares the top two elements (3)
$res = rpn_calc(5, '3 3 <', undef, {}, {}, 'example.org');
ok(!$res, '< compares the top two elements (3)');

# at 85 tests



my $xxx = "
# 
$res = rpn_calc(, '', undef, {}, {}, 'example.org');
ok(, '');
";
