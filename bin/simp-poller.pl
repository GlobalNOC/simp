#!/usr/bin/perl -I ../lib 
#--- SNMP data collector
#---   config file is used to define which parts of mib tree to collect
#---   the set of hosts do poll and how many processes to use to do so.
#---   nodes are divided up amongst processes in simple 1 in N fashion. 
use strict;
use warnings;

use Getopt::Long;
use GRNOC::Simp::Poller;

sub usage {
    print "Usage: $0 [--config <file path>]  [--hosts <file path>] [--logging <file path>] [--nofork]\n";
    exit( 1 );
}


use constant DEFAULT_CONFIG_FILE  => '/etc/simp/config.xml';
use constant DEFAULT_HOSTS_FILE   => '/etc/simp/hosts.d';
use constant DEFAULT_LOGGING_FILE => '/etc/simp/poller_logging.conf';

my $config_file = DEFAULT_CONFIG_FILE;
my $hosts_file  = DEFAULT_HOSTS_FILE;
my $logging     = DEFAULT_LOGGING_FILE;
my $nofork;
my $help;

GetOptions( 'config=s'  => \$config_file,
            'hosts=s'   => \$hosts_file,
 	    'logging=s' => \$logging,
	    'nofork'    => \$nofork,
            'help|h|?'  => \$help ) 

or usage();

usage() if $help;


my $poller = GRNOC::Simp::Poller->new(
			config_file    => $config_file,
                        hosts_file     => $hosts_file,
                        logging_file   => $logging,
			daemonize      => !$nofork );

$poller->start();

