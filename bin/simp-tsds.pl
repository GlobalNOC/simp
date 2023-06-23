#!/usr/bin/perl
use strict;
use warnings;

use Getopt::Long;

use GRNOC::Simp::TSDS;

sub usage {
    print("$0 [--config <config_file>] [--collections <dir path>] [--validation <dir path>] [--logging <logging_file>] [--pidfile pidfile]\n");
    print("\t[--status <dir path>] [--group <group>] [--user <user>]\n");
    print("\t--nofork - Do not daemonize\n");
    exit(1);
}

use constant {
    DEFAULT_CONFIG_FILE     => '/etc/simp/tsds/config.xml',
    DEFAULT_LOGGING_FILE    => '/etc/simp/tsds/logging.conf',
    DEFAULT_PID_FILE        => '/var/run/simp-tsds.pid',
    DEFAULT_COLLECTIONS_DIR => '/etc/simp/tsds/collections.d/',
    DEFAULT_VALIDATION_DIR  => '/etc/simp/tsds/validation.d/',
    DEFAULT_STATUS_DIR      => '/var/lib/grnoc/simp-tsds/',
};

my $config_file     = DEFAULT_CONFIG_FILE;
my $logging_file    = DEFAULT_LOGGING_FILE;
my $pid_file        = DEFAULT_PID_FILE;
my $collections_dir = DEFAULT_COLLECTIONS_DIR;
my $validation_dir  = DEFAULT_VALIDATION_DIR;
my $status_dir      = DEFAULT_STATUS_DIR;
my $nofork          = 0;
my $run_group;
my $run_user;
my $help;

GetOptions(
    'config=s'      => \$config_file,
    'logging=s'     => \$logging_file,
    'collections=s' => \$collections_dir,
    'validation=s'  => \$validation_dir,
    'status=s'      => \$status_dir,
    'pidfile=s'     => \$pid_file,
    'nofork'        => \$nofork,
    'group=s'       => \$run_group,
    'user=s'        => \$run_user,
    'help|h|?'      => \$help
) or usage();

usage() if ($help);

my $collector = GRNOC::Simp::TSDS->new(
    config_file     => $config_file,
    logging_file    => $logging_file,
    pid_file        => $pid_file,
    collections_dir => $collections_dir,
    validation_dir  => $validation_dir,
    status_dir      => $status_dir,
    daemonize       => !$nofork,
    run_user        => $run_user,
    run_group       => $run_group
);

$collector->start();