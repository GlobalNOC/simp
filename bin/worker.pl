#!/usr/bin/perl -I ../lib

use strict;
use warnings;

use GRNOC::Log;
use GRNOC::Config;
use GRNOC::Simp::Data::Worker;

my $grnoc_log = GRNOC::Log->new( config => '../logging.conf' );
my $logger = GRNOC::Log->get_logger();

# create and store config object
my $config = GRNOC::Config->new( config_file => '../simpDataConfig.xml',
				 force_array => 0 );

# create worker in this process
my $worker = GRNOC::Simp::Data::Worker->new( config    => $config,
					     logger    => $logger,
					     worker_id => 1 );

$worker->start();
