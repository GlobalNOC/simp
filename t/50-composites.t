#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_details deep_diag num any code);
use Test::More;
use Test::MockModule;
use Test::MockObject;
use FindBin;
use lib "$FindBin::Bin/lib";
use SimpTesting;

# Loads a JSON file containing the expected output for a composite
sub load_expected {
    my $composite = shift;

    my $path = $FindBin::Bin . "/conf/data_sets/output/$composite.json";

    open(my $file, "<", $path) or die("\nCould not open file: $path");
    my $contents = join("\n", <$file>);
    close($file);

    my $json = JSON::XS::decode_json($contents);
}

# Get every composite name from the composite configs in t/conf/composites/
my @composites = map {$_ =~ m/.*\/(.*)\.xml/} glob($FindBin::Bin . '/conf/composites/*.xml');

# Declare one test per composite
plan tests => scalar(@composites);

# Run a test for every composite
for my $composite (@composites) {
    
    warn "\n";

    # Create a new test instance
    my $test = SimpTesting->new('data_set_name' => $composite);

    # Get the expected output data
    my $expect = load_expected($composite);

    # Get the nodes we want data for from the expected data
    my @nodes = keys %$expect;

    # Get the actual output data from Simp.Comp
    my $got = $test->comp_get(\@nodes, $composite);

    # Init the comparison results variables
    my $ok;
    my $stack;

    # Compare the data returned from Simp.Comp with the expected data
    ($ok, $stack) = cmp_details($got, $expect);

    # Display stack details when a test failed or confirm it passed
    if ($ok) {
        warn "[PASSED] ($composite)";
    }
    else {
        warn "[FAILED] ($composite): " . Dumper(deep_diag($stack));
    }

    # Test that the result of the comparison is good
    ok($ok, "Data returned by Simp.Comp for \"$composite\" does not match the expected output");
}
