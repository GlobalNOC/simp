#!/usr/bin/perl

use Test::More tests => 5;

BEGIN {
        use_ok( 'GRNOC::Simp' );
        use_ok( 'GRNOC::Simp::Poller' );
        use_ok( 'GRNOC::Simp::Poller::Worker' );
        use_ok( 'GRNOC::Simp::Data');
        use_ok( 'GRNOC::Simp::Data::Worker' );
}
