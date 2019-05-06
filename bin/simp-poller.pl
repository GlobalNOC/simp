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
    my $text = <<"EOM";
Usage: $0 [--config <file path>]  [--hosts <file path>]
    [--logging <file path>] [--status <dir path>] [--nofork]
    [--user <user name>] [--group <group name>]
EOM
    print $text;
    exit( 1 );
}


use constant DEFAULT_CONFIG_FILE  => '/etc/simp/config.xml';
use constant DEFAULT_LOGGING_FILE => '/etc/simp/poller_logging.conf';
use constant DEFAULT_HOSTS_DIR    => '/etc/simp/hosts.d/';
use constant DEFAULT_STATUS_DIR   => '/var/lib/simp/poller/';

my $config_file = DEFAULT_CONFIG_FILE;
my $logging     = DEFAULT_LOGGING_FILE;
my $hosts_dir   = DEFAULT_HOSTS_DIR;
my $status_dir  = DEFAULT_STATUS_DIR;
my $nofork;
my $help;
my $username;
my $groupname;

GetOptions( 'config=s'  => \$config_file,
            'logging=s' => \$logging,
            'hosts=s'   => \$hosts_dir,
            'status=s'  => \$status_dir,
            'nofork'    => \$nofork,
            'user=s'    => \$username,
            'group=s'   => \$groupname,
            'help|h|?'  => \$help ) 

or usage();

usage() if $help;


my $poller = GRNOC::Simp::Poller->new(
             config_file    => $config_file,
             logging_file   => $logging,
             hosts_dir      => $hosts_dir,
             status_dir     => $status_dir,
             run_user       => $username,
             run_group      => $groupname,
             daemonize      => !$nofork );

$poller->start();
