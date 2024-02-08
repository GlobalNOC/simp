#!/usr/bin/perl

#
# Daemon script
# Creates and starts Master object
#

use strict;
use warnings;

use lib '/opt/grnoc/venv/simp/lib/perl5';

use Getopt::Long;
use Log::Log4perl;

use GRNOC::Simp::TSDS::Master;

my $config          = '/etc/simp/tsds/config.xml';
my $logging         = '/etc/simp/tsds/logging.conf';
my $collections_dir = '/etc/simp/tsds/collections.d/';
my $validation_dir  = '/etc/simp/tsds/validation.d/';
my $pidfile         = '/var/run/simp-tsds.pid';
my $nofork          = 0;
my $run_group;
my $run_user;
my $help;

GetOptions(
    'config=s'      => \$config,
    'logging=s'     => \$logging,
    'collections=s' => \$collections_dir,
    'validation=s'  => \$validation_dir,
    'pidfile=s'     => \$pidfile,
    'nofork'        => \$nofork,
    'group=s'       => \$run_group,
    'user=s'        => \$run_user,
    'help|h|?'      => \$help
);

usage() if ($help);

Log::Log4perl::init($logging);

my $collector = GRNOC::Simp::TSDS::Master->new(
    config          => $config,
    collections_dir => $collections_dir,
    validation_dir  => $validation_dir,
    pidfile         => $pidfile,
    daemonize       => !$nofork,
    run_user        => $run_user,
    run_group       => $run_group
);

$collector->start();

sub usage
{
    print
      "$0 [--config <config_file>] [--collections <dir path>] [--validation <dir path>] [--logging <logging_file>] [--pidfile pidfile]\n";
    print "\t[--group <group>] [--user <user>]\n";
    print "\t--nofork - Do not daemonize\n";
    exit(1);
}
