#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 10;
use lib './lib';

BEGIN
{
    use_ok('GRNOC::Simp::Poller');
    use_ok('GRNOC::Simp::Poller::Worker');
    use_ok('GRNOC::Simp::Data');
    use_ok('GRNOC::Simp::Data::Worker');
    use_ok('GRNOC::Simp::Comp');
    use_ok('GRNOC::Simp::Comp::Worker');
    use_ok('GRNOC::Simp::TSDS');
    use_ok('GRNOC::Simp::TSDS::Pusher');
    use_ok('GRNOC::Simp::TSDS::Worker');
    use_ok('GRNOC::Simp::TSDS::Master');
}
