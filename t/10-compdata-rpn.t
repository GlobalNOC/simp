#!/usr/bin/perl

use strict;
use warnings;

use lib './lib';

use Test::More tests => 131;

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


# Basics

$res = rpn_calc('hello world', '', undef, {}, {}, 'example.org');
ok($res eq 'hello world', 'empty program returns passed-in value');

@res = rpn_calc(5, '3', undef, {}, {}, 'example.org');
ok((scalar(@res) == 1) && ($res[0] == 3), 'only item on top of stack is returned');

$res = rpn_calc(5, '   3   ', undef, {}, {}, 'example.org');
ok($res == 3, "whitespace in program doesn't interfere with program");


# Literal operands

$res = rpn_calc(5, '-27', undef, {}, {}, 'example.org');
ok($res == -27 , 'negative numbers can be pushed');

$res = rpn_calc(5, '  2.375 ', undef, {}, {}, 'example.org');
ok($res == 2.375, 'numbers with decimal points can be pushed');

$res = rpn_calc(5, '"a string"', undef, {}, {}, 'example.org');
ok($res eq 'a string', 'strings can be pushed');

$res = rpn_calc(5, "'another string'", undef, {}, {}, 'example.org');
ok($res eq 'another string', 'single-quoted strings can be pushed');

$res = rpn_calc(5, '  "xyzzy"  ', undef, {}, {}, 'example.org');
ok($res eq 'xyzzy', "whitespace in program doesn't interfere with string literals");

$res = rpn_calc(5, ' "ab\\"cde\\\'fgh\\\\ijk\\lmn\\\\\\o\\p" ', undef, {}, {}, 'example.org');
ok($res eq 'ab"cde\'fgh\\ijklmn\\op', 'backslash escapes work in double-quoted strings');

$res = rpn_calc(5, " 'ab\\\"cde\\'fgh\\\\ijk\\lmn\\\\\\o\\p'  ", undef, {}, {}, 'example.org');
ok($res eq 'ab"cde\'fgh\\ijklmn\\op', 'backslash escapes work in single-quoted strings');


# Edge cases for string literals

$res = rpn_calc(5, '  "ab\\cde ', undef, {}, {}, 'example.org');
ok($res eq 'abcde ', 'unterminated double-quoted string literals are handled (case 1)');

$res = rpn_calc(5, ' "ab\\"cde\\\\', undef, {}, {}, 'example.org');
ok($res eq 'ab"cde\\', 'unterminated double-quoted string literals are handled (case 2)');

$res = rpn_calc(5, ' "ab\\"cde\\"', undef, {}, {}, 'example.org');
ok($res eq 'ab"cde"', 'unterminated double-quoted string literals are handled (case 3)');

$res = rpn_calc(5, ' "abcde\\', undef, {}, {}, 'example.org');
ok($res eq 'abcde', 'unterminated double-quoted string literals are handled (case 4)');

$res = rpn_calc(5, "  'ab\\cde ", undef, {}, {}, 'example.org');
ok($res eq 'abcde ', 'unterminated single-quoted string literals are handled (case 1)');

$res = rpn_calc(5, " 'ab\\'cde\\\\", undef, {}, {}, 'example.org');
ok($res eq 'ab\'cde\\', 'unterminated single-quoted string literals are handled (case 2)');

$res = rpn_calc(5, " 'ab\\'cde\\'", undef, {}, {}, 'example.org');
ok($res eq 'ab\'cde\'', 'unterminated single-quoted string literals are handled (case 3)');

$res = rpn_calc(5, " 'abcde\\", undef, {}, {}, 'example.org');
ok($res eq 'abcde', 'unterminated single-quoted string literals are handled (case 4)');


# Associated-value, host-variable, and hostname tokens

$res = rpn_calc(5, '$a', undef, $val_set, $results_test, 'example.org');
ok($res == 23, 'associated-value tokens push proper value (1)');

$res = rpn_calc(5, ' $a ', undef, $val_set, $results_test, 'example.org');
ok($res == 23, 'associated-value tokens push proper value (2)');

$res = rpn_calc(5, ' $abc', undef, $val_set, $results_test, 'example.org');
ok($res eq 'zyx', 'associated-value tokens push proper value (3)');

$res = rpn_calc(5, '$2a ', undef, $val_set, $results_test, 'example.org');
ok($res == 99, 'associated-value tokens push proper value (4)');

$res = rpn_calc(5, ' $a2c ', undef, $val_set, $results_test, 'example.org');
ok($res == 98, 'associated-value tokens push proper value (5)');

$res = rpn_calc(5, ' $bb ', undef, $val_set, $results_test, 'example.org');
ok($res == 97, 'associated-value tokens push proper value (6)');

$res = rpn_calc(5, ' $not_present ', undef, $val_set, $results_test, 'example.org');
ok(!defined($res), "associated-value tokens push undef when value doesn't exist");

$res = rpn_calc(5, '#a', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYa', 'associated-values tokens push proper value (1)');

$res = rpn_calc(5, ' #a ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYa', 'host-variable tokens push proper value (2)');

$res = rpn_calc(5, ' #abc', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYabc', 'host-variable tokens push proper value (3)');

$res = rpn_calc(5, '#2a ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XY2a', 'host-variable tokens push proper value (4)');

$res = rpn_calc(5, ' #a2c ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYa2c', 'host-variable tokens push proper value (5)');

$res = rpn_calc(5, ' #bb ', undef, $val_set, $results_test, 'example.org');
ok($res eq 'XYbb', 'host-variable tokens push proper value (6)');

$res = rpn_calc(5, ' #not_present ', undef, $val_set, $results_test, 'example.org');
ok(!defined($res), "host-variable tokens push undef when value doesn't exist");

$res = rpn_calc(5, '@', undef, $val_set, $results_test, 'example.org');
ok($res eq 'example.org', 'hostname token pushes hostname');


# Basic arithmetic functions

$res = rpn_calc(5, ' 7  +', undef, {}, {}, 'example.org');
ok($res == 12, '+ adds two numbers');

$res = rpn_calc(5, '7 100 +', undef, {}, {}, 'example.org');
ok($res == 107, '+ *only* adds two numbers');

$res = rpn_calc(5, '+', undef, {}, {}, 'example.org');
ok(!defined($res), '+ returns undef if only one item on the stack');

$res = rpn_calc(5, 'pop +', undef, {}, {}, 'example.org');
ok(!defined($res), '+ returns undef if zero items on the stack');

$res = rpn_calc(10, '5 3 -', undef, {}, {}, 'example.org');
ok($res == 2, '- subtracts top of stack from next element down');

$res = rpn_calc(5, '37 3 *', undef, {}, {}, 'example.org');
ok($res == 111, '* multiplies two numbers, and only two numbers');

$res = rpn_calc(19, '37 10 5 /', undef, {}, {}, 'example.org');
ok($res == 2, '/ divides top of stack from next element down');

$res = rpn_calc(5, '0 /', undef, {}, {}, 'example.org');
ok(!defined($res), 'divide by zero yields undef');

$res = rpn_calc(5, '0 5 /', undef, {}, {}, 'example.org');
ok($res == 0, 'dividing zero yields a result :)');

$res = rpn_calc(5.5, '-1.25 +', undef, {}, {}, 'example.org');
ok($res == 4.25, '+ works with decimals');

$res = rpn_calc(6, '4 /', undef, {}, {}, 'example.org');
ok($res == 1.5, '/ is not integer division');

$res = rpn_calc(35, '5 3 %', undef, {}, {}, 'example.org');
ok($res == 2, '% takes modulus of next-to-top element divided by top element');

$res = rpn_calc(8, '0 %', undef, {}, {}, 'example.org');
ok(!defined($res), 'modulo zero yields undef');


# Other numeric functions

$res = rpn_calc(5, '10 ln', undef, {}, {}, 'example.org');
ok(($res - 2.3026) < 1e-3, 'ln takes natural logarithm of top of stack (1)');

$res = rpn_calc(5, '1 ln', undef, {}, {}, 'example.org');
ok($res == 0, 'ln takes natural logarithm of top of stack (2)');

$res = rpn_calc(5, '0 ln', undef, {}, {}, 'example.org');
ok(!defined($res), 'natural logarithm of zero yields undef');

$res = rpn_calc(-5, 'ln', undef, {}, {}, 'example.org');
ok(!defined($res), 'natural logarithm of negative number yields undef');

$res = rpn_calc(7, '100 log10', undef, {}, {}, 'example.org');
ok(($res - 2) < 1e-4, 'log10 yields base-10 logarithm of top of stack');

$res = rpn_calc(5, '0 log10', undef, {}, {}, 'example.org');
ok(!defined($res), 'log10 of zero yields undef');

$res = rpn_calc(-5, 'log10', undef, {}, {}, 'example.org');
ok(!defined($res), 'log10 of negative number yields undef');

$res = rpn_calc(3, '0 exp', undef, {}, {}, 'example.org');
ok($res == 1, 'exp calculates base-e exponentation (1)');

$res = rpn_calc(3, '1 exp', undef, {}, {}, 'example.org');
ok(($res > 2.71828) && ($res < 2.71829), 'exp calculates base-e exponentation (2)');

$res = rpn_calc(25, '2.5 3 pow', undef, {}, {}, 'example.org');
ok(abs($res - 15.625) < 1e-6, 'pow yields exponentation (1)');

$res = rpn_calc(25, '9 -0.5 pow', undef, {}, {}, 'example.org');
ok(abs($res - 0.3333333333) < 1e-6, 'pow yields exponentation (2)');


# Push-undef function

$res = rpn_calc(5, '6 7 8 _', undef, {}, {}, 'example.org');
ok(!defined($res), '_ pushes undef');

$res = rpn_calc(5, '6 7 8 _ pop', undef, {}, {}, 'example.org');
ok($res == 8, '_ pushes only one undef');


# defined? predicate

$res = rpn_calc(3, '4 5 _ 7 defined? ', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given a non-zero number');

$res = rpn_calc(3, '4 5 _ 0 defined?', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given zero');

$res = rpn_calc(3, '"a" "b" _ "c" defined?', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given a non-empty string');

$res = rpn_calc(3, '"a" "b" _ "" defined?', undef, {}, {}, 'example.org');
ok($res, 'defined? returns true when given an empty string');

$res = rpn_calc(3, '"a" "b" _ defined?', undef, {}, {}, 'example.org');
ok(!$res, 'undef? returns false when given undef');


# Numerical-comparison predicates

$res = rpn_calc(5, '8 8 ==', undef, {}, {}, 'example.org');
ok($res, '== returns true when top two numbers are equal (1)');

$res = rpn_calc(5, '-8 "-8" ==', undef, {}, {}, 'example.org');
ok($res, '== returns true when top two numbers are equal (2)');

$res = rpn_calc(5, '5 7 ==', undef, {}, {}, 'example.org');
ok(!$res, '== returns false when top two elements are numerically unequal');

$res = rpn_calc(5, '_ _ ==', undef, {}, {}, 'example.org');
ok($res, '== returns true when top two elements are both undef');

$res = rpn_calc(0, '_ 0 ==', undef, {}, {}, 'example.org');
ok(!$res, '== returns false when one but not both of the top two elements are undef');

$res = rpn_calc(5, '8 9 !=', undef, {}, {}, 'example.org');
ok($res, '!= returns true when top two numbers are unequal (1)');

$res = rpn_calc(5, '-8 "-9" !=', undef, {}, {}, 'example.org');
ok($res, '!= returns true when top two numbers are unequal (2)');

$res = rpn_calc(5, '7 7 !=', undef, {}, {}, 'example.org');
ok(!$res, '!= returns false when top two elements are numerically equal');

$res = rpn_calc(5, '_ _ !=', undef, {}, {}, 'example.org');
ok(!$res, '!= returns false when top two elements are both undef');

$res = rpn_calc(0, '_ 0 !=', undef, {}, {}, 'example.org');
ok($res, '!= returns true when one but not both of the top two elements are undef');

$res = rpn_calc(5, '8 3 >', undef, {}, {}, 'example.org');
ok($res, '> compares the top two elements (1)');

$res = rpn_calc(5, '-8 3 >', undef, {}, {}, 'example.org');
ok(!$res, '> compares the top two elements (2)');

$res = rpn_calc(5, '3 3 >', undef, {}, {}, 'example.org');
ok(!$res, '> compares the top two elements (3)');

$res = rpn_calc(5, '8 3 >=', undef, {}, {}, 'example.org');
ok($res, '>= compares the top two elements (1)');

$res = rpn_calc(5, '-8 3 >=', undef, {}, {}, 'example.org');
ok(!$res, '>= compares the top two elements (2)');

$res = rpn_calc(5, '3 3 >=', undef, {}, {}, 'example.org');
ok($res, '>= compares the top two elements (3)');

$res = rpn_calc(5, '8 3 <=', undef, {}, {}, 'example.org');
ok(!$res, '<= compares the top two elements (1)');

$res = rpn_calc(5, '-8 3 <=', undef, {}, {}, 'example.org');
ok($res, '<= compares the top two elements (2)');

$res = rpn_calc(5, '3 3 <=', undef, {}, {}, 'example.org');
ok($res, '<= compares the top two elements (3)');

$res = rpn_calc(5, '8 3 <', undef, {}, {}, 'example.org');
ok(!$res, '< compares the top two elements (1)');

$res = rpn_calc(5, '-8 3 <', undef, {}, {}, 'example.org');
ok($res, '< compares the top two elements (2)');

$res = rpn_calc(5, '3 3 <', undef, {}, {}, 'example.org');
ok(!$res, '< compares the top two elements (3)');


# Boolean functions

$res = rpn_calc(8, '0 0 and ', undef, {}, {}, 'example.org');
ok(!$res, 'AND truth table 0 0 -> 0');

$res = rpn_calc(8, '0 1 and', undef, {}, {}, 'example.org');
ok(!$res, 'AND truth table 0 1 -> 0');

$res = rpn_calc(8, '1 0 and', undef, {}, {}, 'example.org');
ok(!$res, 'AND truth table 1 0 -> 0');

$res = rpn_calc(8, '1 1 and', undef, {}, {}, 'example.org');
ok($res, 'AND truth table 1 1 -> 1');

$res = rpn_calc(8, '0 0 or ', undef, {}, {}, 'example.org');
ok(!$res, 'OR truth table 0 0 -> 0');

$res = rpn_calc(8, '0 1 or', undef, {}, {}, 'example.org');
ok($res, 'OR truth table 0 1 -> 1');

$res = rpn_calc(8, '1 0 or', undef, {}, {}, 'example.org');
ok($res, 'OR truth table 1 0 -> 1');

$res = rpn_calc(8, '1 1 or', undef, {}, {}, 'example.org');
ok($res, 'OR truth table 1 1 -> 1');

$res = rpn_calc(8, '1 0 not', undef, {}, {}, 'example.org');
ok($res, 'NOT truth table 0 -> 1');

$res = rpn_calc(8, '1 1 not', undef, {}, {}, 'example.org');
ok(!$res, 'NOT truth table 1 -> 0');


# "ifelse" eager-evaluation trinary operator

$res = rpn_calc(1, '2 < "a" "b" ifelse', undef, {}, {}, 'example.org');
ok($res eq 'a', '"true a b ifelse" returns a');

$res = rpn_calc(2, '1 < "a" "b" ifelse', undef, {}, {}, 'example.org');
ok($res eq 'b', '"false a b ifelse" returns b');


# String-related functions

$res = rpn_calc('abc', '"def" "ghi" concat', undef, {}, {}, 'example.org');
ok($res eq 'defghi', 'concat string-concatenates the top two entries on the stack');

$res = rpn_calc('abc', '"def" _ concat', undef, {}, {}, 'example.org');
ok(defined($res) && ($res eq 'def'), 'concat treats undef as empty string (1)');

$res = rpn_calc('abc', '_ "def" concat', undef, {}, {}, 'example.org');
ok(defined($res) && ($res eq 'def'), 'concat treats undef as empty string (2)');

$res = rpn_calc('abc', '_ _ concat', undef, {}, {}, 'example.org');
ok(defined($res) && ($res eq ''), 'concat treats undef as empty string (3)');

$res = rpn_calc('abc', 'concat', undef, {}, {}, 'example.org');
ok(defined($res) && ($res eq 'abc'), 'concat treats empty stack as empty string (1)');

$res = rpn_calc('abc', 'pop concat', undef, {}, {}, 'example.org');
ok(defined($res) && ($res eq ''), 'concat treats empty stack as empty string (2)');

$res = rpn_calc('abbcccd', '"(b+c+)" match', undef, {}, {}, 'example.org');
ok(defined($res) && $res, 'match runs regular expression match');

$res = rpn_calc('testTESTabc', '"[a-z]([A-Z]+)([a-z]+)$" match', undef, {}, {}, 'example.org');
ok($res eq 'TEST', 'match returns first matched group');

$res = rpn_calc('testTESTabc', '"(z)" match', undef, {}, {}, 'example.org');
ok(!defined($res), 'match returns undef if nothing matched');

$res = rpn_calc('a.example.org', '"^[-a-z0-9_]+\.([-a-z0-9_]+)\..*$"    "X$1Z"    replace', undef, {}, {}, 'example.org');
ok($res eq 'XexampleZ', 'replace does regexp match-and-replacement (1)');

$res = rpn_calc('a.example.org', '"exam"    "test"    replace', undef, {}, {}, 'example.org');
ok($res eq 'a.testple.org', 'replace does regexp match-and-replacement (2)');

$res = rpn_calc('a.example.org', '_    "test"    replace', undef, {}, {}, 'example.org');
ok(!defined($res), 'replace returns undef if any argument is undef');

$res = rpn_calc('a.example.org', '"test"    replace', undef, {}, {}, 'example.org');
ok(!defined($res), 'replace returns undef if fewer than three items on the stack');


# Stack-manipulation functions

$res = rpn_calc(5, '6 7 8 pop', undef, {}, {}, 'example.org');
ok($res == 7, 'pop removes top element from stack');

$res = rpn_calc(5, '6 pop pop pop pop', undef, {}, {}, 'example.org');
ok(!defined($res), 'pop is a no-op on an empty stack');

$res = rpn_calc(3, '6 "a" exch', undef, {}, {}, 'example.org');
ok($res == 6, 'exch swaps top two elements of stack (1)');

$res = rpn_calc(3, '6 "a" exch pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'exch swaps top two elements of stack (2)');

$res = rpn_calc(3, '6 "a" exch pop pop', undef, {}, {}, 'example.org');
ok($res == 3, 'exch swaps only top two elements of stack (3)');

$res = rpn_calc(6, 'exch', undef, {}, {}, 'example.org');
ok($res == 6, 'exch is a no-op when there are fewer than two elements on the stack');

$res = rpn_calc('x', '"a" dup', undef, {}, {}, 'example.org');
ok($res eq 'a', 'dup copies the top element (1)');

$res = rpn_calc('x', '"a" dup pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'dup copies the top element (2)');

$res = rpn_calc('x', '"a" dup pop pop', undef, {}, {}, 'example.org');
ok($res eq 'x', 'dup copies the top element (3)');

$res = rpn_calc('x', 'pop dup', undef, {}, {}, 'example.org');
ok(!defined($res), 'dup is a no-op on an empty stack');

$res = rpn_calc('c', '"b" "a" 1 index', undef, {}, {}, 'example.org');
ok($res eq 'a', 'index fetches elements, indexed from the top of the stack (1a)');

$res = rpn_calc('c', '"b" "a" 1 index pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'index fetches elements, indexed from the top of the stack (1b)');

$res = rpn_calc('c', '"b" "a" 2 index', undef, {}, {}, 'example.org');
ok($res eq 'b', 'index fetches elements, indexed from the top of the stack (2a)');

$res = rpn_calc('c', '"b" "a" 2 index pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'index fetches elements, indexed from the top of the stack (2b)');

$res = rpn_calc('c', '"b" "a" 3 index', undef, {}, {}, 'example.org');
ok($res eq 'c', 'index fetches elements, indexed from the top of the stack (3a)');

$res = rpn_calc('c', '"b" "a" 3 index pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'index fetches elements, indexed from the top of the stack (3b)');

$res = rpn_calc('c', '"b" "a" 0 index', undef, {}, {}, 'example.org');
ok(!defined($res), 'index-0 yields undef (a)');

$res = rpn_calc('c', '"b" "a" 0 index pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'index-0 yields undef (b)');

$res = rpn_calc('c', '"b" "a" 7 index', undef, {}, {}, 'example.org');
ok(!defined($res), 'indexes larger than stack size yield undef (a)');

$res = rpn_calc('c', '"b" "a" 7 index pop', undef, {}, {}, 'example.org');
ok($res eq 'a', 'indexes larger than stack size yield undef (b)');
