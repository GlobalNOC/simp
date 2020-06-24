#!/usr/bin/perl

use strict;
use warnings;

use Data::Dumper;
use Test::Deep qw(cmp_details num any code);
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

# Returns the composite names based on their config files' names
sub get_composites {
    return map {$_ =~ m/.*\/(.*)\.xml/} glob($FindBin::Bin . '/conf/composites/*.xml');
}

# The node name used in test data
my $node = 'acme.grnoc.iu.edu';

# Get every composite name from the composite configs in t/conf/composites/
my @composites = map {$_ =~ m/.*\/(.*)\.xml/} glob($FindBin::Bin . '/conf/composites/*.xml');

# Declare one test per composite
plan tests => scalar(@composites);

# Run a test for every composite
for my $composite (@composites) {

    # Create a new test instance
    my $test = SimpTesting->new('data_set_name' => $composite);

    # Get the expected output data
    my $expected = load_expected($composite);

    # Get the actual output data from Simp.Comp
    my $data = $test->comp_get($node, $composite);

    # Init the comparison results variables
    my $ok;
    my $details;

    # Compare the data returned from Simp.Comp with the expected data
    ($ok, $details) = cmp_details($data, $expected);

    # Test that the result of the comparison is good
    ok($ok, "Data returned by Simp.Comp matches the expected output");

    # Display stack details when a test failed
    if (!$ok) {
        warn "[$composite] FAILED:\n" . Dumper($details);
    }
}
