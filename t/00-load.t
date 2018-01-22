#!/usr/bin/perl

use Test::More tests => 7;

BEGIN {
        use_ok( 'GRNOC::Simp' );
        use_ok( 'GRNOC::Simp::Poller' );
        use_ok( 'GRNOC::Simp::Poller::Worker' );
        use_ok( 'GRNOC::Simp::Data');
        use_ok( 'GRNOC::Simp::Data::Worker' );
        use_ok( 'GRNOC::Simp::CompData');
        use_ok(' GRNOC::Simp::CompData::Worker');
}
