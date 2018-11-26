#!/usr/bin/perl

#
# Daemon script
# Creates and starts Master object
#

use strict;
use warnings;

use Getopt::Long;
use Log::Log4perl;
use GRNOC::Simp::TSDS::Master;

my $conf_file = '/etc/simp/simp-tsds.xml';
my $logging_file = '/etc/simp/simp_tsds_logging.conf';
my $pidfile = '/var/run/simp-tsds.pid';
my $nofork = 0;
my $dir = '/etc/simp/tsds.d';
my $run_group;
my $run_user;
my $help;

GetOptions(
    'config=s' => \$conf_file,
    'logging=s' => \$logging_file,
    'pidfile=s' => \$pidfile,
    'nofork' => \$nofork,
    'group=s' => \$run_group,
    'user=s' => \$run_user,
    'dir=s' => \$dir,
    'help|h|?' => \$help,
);

usage() if ($help); 

Log::Log4perl::init($logging_file);

 my $collector = GRNOC::Simp::TSDS::Master->new(
     config_file => $conf_file,
     pidfile => $pidfile,
     daemonize => !$nofork,
     tsds_dir => $dir,
     run_user => $run_user,
     run_group => $run_group
    );

$collector->start();

sub usage {
    print "$0 [--config <config_file>] [--logging <logging_file>] [--pidfile pidfile]\n";
    print "\t[--group <group>] [--user <user>]\n";
    print "\t--nofork - Do not daemonize\n";
    exit(1);
}
