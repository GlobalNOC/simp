#!/usr/bin/perl -I /opt/grnoc/venv/simp/lib/perl5

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
    [--user <user name>] [--group <group name>]
EOM
    print $text;
    exit(1);
}

use constant {
    DEFAULT_CONFIG_FILE    => '/etc/simp/poller/config.xml',
    DEFAULT_LOGGING_FILE   => '/etc/simp/poller/logging.conf',
    DEFAULT_HOSTS_DIR      => '/etc/simp/poller/hosts.d/',
    DEFAULT_GROUPS_DIR     => '/etc/simp/poller/groups.d/',
    DEFAULT_VALIDATION_DIR => '/etc/simp/poller/validation.d/',
    DEFAULT_STATUS_DIR     => '/var/lib/simp/poller/',
};

my $config_file    = DEFAULT_CONFIG_FILE;
my $logging        = DEFAULT_LOGGING_FILE;
my $hosts_dir      = DEFAULT_HOSTS_DIR;
my $groups_dir     = DEFAULT_GROUPS_DIR;
my $validation_dir = DEFAULT_VALIDATION_DIR;
my $status_dir     = DEFAULT_STATUS_DIR;
my $nofork;
my $help;
my $username;
my $groupname;

GetOptions(
    'config=s'     => \$config_file,
    'logging=s'    => \$logging,
    'hosts=s'      => \$hosts_dir,
    'groups=s'     => \$groups_dir,
    'validation=s' => \$validation_dir,
    'status=s'     => \$status_dir,
    'nofork'       => \$nofork,
    'user=s'       => \$username,
    'group=s'      => \$groupname,
    'help|h|?'     => \$help
) or usage();

usage() if $help;

my $poller = GRNOC::Simp::Poller->new(
    config_file    => $config_file,
    logging_file   => $logging,
    hosts_dir      => $hosts_dir,
    groups_dir     => $groups_dir,
    validation_dir => $validation_dir,
    status_dir     => $status_dir,
    daemonize      => !$nofork,
    run_user       => $username,
    run_group      => $groupname
);

$poller->start();
