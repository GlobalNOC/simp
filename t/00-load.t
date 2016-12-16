#!/usr/bin/perl

use Test::More tests => 9;

BEGIN {
        use_ok( 'VCE' );
        use_ok( 'VCE::Access' );
        use_ok( 'VCE::Services::Access' );
        use_ok( 'VCE::NetworkModel');
        use_ok( 'VCE::Services::Provisioning' );
        use_ok( 'VCE::Services::Switch' );
        use_ok( 'VCE::Device' );
        use_ok( 'VCE::Device::Brocade::MLXe::5_8_0' );
        use_ok( 'VCE::Switch' );
}
