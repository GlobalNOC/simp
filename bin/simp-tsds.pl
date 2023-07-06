#!/usr/bin/perl
use strict;
use warnings;

use File::Spec;
use Getopt::Long;

use GRNOC::Simp::TSDS;

sub usage {
    print("$0 [--config <config_file>] [--collections <dir path>] [--validation <dir path>] [--logging <logging_file>] [--pidfile pidfile]\n");
    print("\t[--status <dir path>] [--tmp <dir path>] [--group <group>] [--user <user>]\n");
    print("\t--nofork - Do not daemonize\n");
    exit(64);
}

# Helper for resolving relative file paths
sub resolve {
    my $path = shift;
    $path = File::Spec->rel2abs($path) if (-e $path);
    $path .= '/' if (-d $path);
    return $path;
}

use constant {
    DEFAULT_CONFIG_FILE     => '/etc/simp/tsds/config.xml',
    DEFAULT_LOGGING_FILE    => '/etc/simp/tsds/logging.conf',
    DEFAULT_PID_FILE        => '/var/run/simp-tsds.pid',
    DEFAULT_TEMP_DIR        => '/tmp/simp/tsds/',
    DEFAULT_COLLECTIONS_DIR => '/etc/simp/tsds/collections.d/',
    DEFAULT_VALIDATION_DIR  => '/etc/simp/tsds/validation.d/',
    DEFAULT_STATUS_DIR      => '/var/lib/simp/tsds/',
};

my $config_file     = DEFAULT_CONFIG_FILE;
my $logging_file    = DEFAULT_LOGGING_FILE;
my $pid_file        = DEFAULT_PID_FILE;
my $temp_dir        = DEFAULT_TEMP_DIR;
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
    'tempdir=s'     => \$temp_dir,
    'nofork'        => \$nofork,
    'group=s'       => \$run_group,
    'user=s'        => \$run_user,
    'help|h|?'      => \$help
) or usage();

usage() if ($help);

my $collector = GRNOC::Simp::TSDS->new(
    config_file     => resolve($config_file),
    logging_file    => resolve($logging_file),
    pid_file        => resolve($pid_file),
    temp_dir        => resolve($temp_dir),
    collections_dir => resolve($collections_dir),
    validation_dir  => resolve($validation_dir),
    status_dir      => resolve($status_dir),
    daemonize       => !$nofork,
    run_user        => $run_user,
    run_group       => $run_group
);

$collector->run();