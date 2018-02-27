#!/usr/bin/perl -I ../lib 
use strict;
use warnings;

#use AnyEvent::Loop;
use Getopt::Long;
use GRNOC::Simp::CompData;

sub usage {
    print "Usage: $0 [--config <file path>] [--logging <file path>] [--nofork]\n";
    exit( 1 );
}


use constant DEFAULT_CONFIG_FILE => '/etc/simp/compDataConfig.xml';
use constant DEFAULT_LOG_FILE    => '/etc/simp/comp_logging.conf';

my $config_file = DEFAULT_CONFIG_FILE;
my $logging     = DEFAULT_LOG_FILE;
my $nofork;
my $help;

GetOptions( 'config=s' => \$config_file,
 	    'logging=s' => \$logging,
	    'nofork' => \$nofork,
            'help|h|?' => \$help );

usage() if $help;


my $data_services = GRNOC::Simp::CompData->new(
			config_file    => $config_file,
                        logging_file   => $logging,
			daemonize      => !$nofork );

$data_services->start();

