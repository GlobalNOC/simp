#!/usr/bin/perl -I /opt/grnoc/venv/simp/lib/perl5

use strict;
use warnings;

use Getopt::Long;
use GRNOC::Simp::Comp;

sub usage
{
    my $text = <<"EOM";
Usage: $0 [--config <file path>] [--logging <file path>] [--composites <composites dir>]
    [--nofork] [--user <user name>] [--group <group name>]
EOM
    print $text;
    exit(1);
}

use constant {
    DEFAULT_CONFIG_FILE    => '/etc/simp/comp/config.xml',
    DEFAULT_LOG_FILE       => '/etc/simp/comp/logging.conf',
    DEFAULT_COMPOSITES_DIR => '/etc/simp/comp/composites.d/',
    DEFAULT_CONFIG_XSD     => '/etc/simp/comp/validation.d/config.xsd',
    DEFAULT_COMPOSITE_XSD  => '/etc/simp/comp/validation.d/composite.xsd',
    DEFAULT_EXPORTER_FILE  => '/etc/simp/exporter/config.xml',
    DEFAULT_EXPORTER_XSD  =>  '/etc/simp/exporter/validation.d/config.xml'
};

my $config        = DEFAULT_CONFIG_FILE;
my $logging       = DEFAULT_LOG_FILE;
my $composites    = DEFAULT_COMPOSITES_DIR;
my $config_xsd    = DEFAULT_CONFIG_XSD;
my $composite_xsd = DEFAULT_COMPOSITE_XSD;
my $exporter      = DEFAULT_EXPORTER_FILE;
my $exporter_val  = DEFAULT_EXPORTER_XSD;
my $nofork;
my $help;
my $username;
my $groupname;

GetOptions(
    'config_file=s'    => \$config,
    'exporter_file=s'  => \$exporter,
    'exporter_val=s'   => \$exporter_val,
    'logging_file=s'   => \$logging,
    'composites_dir=s' => \$composites,
    'nofork'           => \$nofork,
    'user=s'           => \$username,
    'group=s'          => \$groupname,
    'help|h|?'         => \$help
);

usage() if $help;

my $data_services = GRNOC::Simp::Comp->new(
    config_file    => $config,
    exporter_file  => $exporter,
    exporter_val   => $expoter_val,
    logging_file   => $logging,
    composites_dir => $composites,
    run_user       => $username,
    run_group      => $groupname,
    config_xsd     => $config_xsd,
    composite_xsd  => $composite_xsd,
    daemonize      => !$nofork
);

$data_services->start();
