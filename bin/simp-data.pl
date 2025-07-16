#!/usr/bin/perl -I /opt/grnoc/venv/simp/lib/perl5

use strict;
use warnings;

use Getopt::Long;
use GRNOC::Simp::Data;

sub usage
{
    my $text = <<"EOM";
Usage: $0 [--config <file path>] [--logging <file path>] [--validation <file path>]
    [--nofork] [--user <user name>] [--group <group name>]
EOM
    print $text;
    exit(1);
}

use constant {
    DEFAULT_CONFIG_FILE     => '/etc/simp/data/config.xml',
    DEFAULT_LOG_FILE        => '/etc/simp/data/logging.conf',
    DEFAULT_VALIDATION_FILE => '/etc/simp/data/validation.d/config.xsd',
    DEFAULT_EXPORTER_FILE  => '/etc/simp/exporter/config.xml',
    DEFAULT_EXPORTER_XSD  =>  '/etc/simp/exporter/validation.d/config.xml'
};

my $config       = DEFAULT_CONFIG_FILE;
my $exporter     = DEFAULT_EXPORTER_FILE;
my $exporter_val = DEFAULT_EXPORTER_XSD;
my $logging      = DEFAULT_LOG_FILE;
my $validation   = DEFAULT_VALIDATION_FILE;
my $nofork;
my $help;
my $username;
my $groupname;

GetOptions(
    'config=s'     => \$config,
    'exporter=s'   => \$exporter,
    'exporter_val' => \$exporter_val,
    'logging=s'    => \$logging,
    'validation'   => \$validation,
    'nofork'       => \$nofork,
    'user=s'       => \$username,
    'group=s'      => \$groupname,
    'help|h|?'     => \$help
);

usage() if $help;

my $data_services = GRNOC::Simp::Data->new(
    config_file     => $config,
    exporter_file   => $exporter_file,
    exporter_val    => $exporter_val,
    logging_file    => $logging,
    validation_file => $validation,
    run_user        => $username,
    run_group       => $groupname,
    daemonize       => !$nofork
);

$data_services->start();
