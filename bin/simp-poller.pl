#!/usr/bin/perl

#--- SNMP data collector
#---   config file is used to define which parts of mib tree to collect
#---   the set of hosts do poll and how many processes to use to do so.
#---   nodes are divided up amongst processes in simple 1 in N fashion.

use strict;
use warnings;

use Getopt::Long;
use GRNOC::Simp::Poller;

sub usage
{
    my $text = <<"EOM";
Usage: $0 [--config <file path>]  [--hosts <dir path>] [--groups <dir path>]
    [--logging <file path>] [--validation <dir path>] [--status <dir path>] [--nofork]
    [--user <user name>] [--group <group name>] [--pid_file <file path>]
EOM
    print $text;
    exit(1);
}

use constant {
    DEFAULT_PID_FILE       => '/var/run/simp_poller.pid',
    DEFAULT_CONFIG_FILE    => '/etc/simp/poller/config.xml',
    DEFAULT_LOGGING_FILE   => '/etc/simp/poller/logging.conf',
    DEFAULT_HOSTS_DIR      => '/etc/simp/poller/hosts.d/',
    DEFAULT_GROUPS_DIR     => '/etc/simp/poller/groups.d/',
    DEFAULT_VALIDATION_DIR => '/etc/simp/poller/validation.d/',
    DEFAULT_STATUS_DIR     => '/var/lib/simp/poller/',
};

my $pid_file       = DEFAULT_PID_FILE;
my $config_file    = DEFAULT_CONFIG_FILE;
my $logging        = DEFAULT_LOGGING_FILE;
my $hosts_dir      = DEFAULT_HOSTS_DIR;
my $groups_dir     = DEFAULT_GROUPS_DIR;
my $validation_dir = DEFAULT_VALIDATION_DIR;
my $status_dir     = DEFAULT_STATUS_DIR;
my $nofork;
my $help;
my $user_name;
my $group_name;

GetOptions(
    'pid_file=s'   => \$pid_file,
    'config=s'     => \$config_file,
    'logging=s'    => \$logging,
    'hosts=s'      => \$hosts_dir,
    'groups=s'     => \$groups_dir,
    'validation=s' => \$validation_dir,
    'status=s'     => \$status_dir,
    'nofork'       => \$nofork,
    'user=s'       => \$user_name,
    'group=s'      => \$group_name,
    'help|h|?'     => \$help
) or usage();

usage() if $help;

my $poller_args = {
    pid_file       => $pid_file,
    config_file    => $config_file,
    logging_file   => $logging,
    hosts_dir      => $hosts_dir,
    groups_dir     => $groups_dir,
    validation_dir => $validation_dir,
    status_dir     => $status_dir,
    daemonize      => !$nofork,
    run_user       => $user_name,
    run_group      => $group_name
};

GRNOC::Simp::Poller->new($poller_args)->start();

