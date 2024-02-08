#!/usr/bin/perl

use strict;
use warnings;

use lib '/opt/grnoc/venv/simp/lib/perl5';

#use AnyEvent::Loop;
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
};

my $config        = DEFAULT_CONFIG_FILE;
my $logging       = DEFAULT_LOG_FILE;
my $composites    = DEFAULT_COMPOSITES_DIR;
my $config_xsd    = DEFAULT_CONFIG_XSD;
my $composite_xsd = DEFAULT_COMPOSITE_XSD;
my $nofork;
my $help;
my $username;
my $groupname;

GetOptions(
    'config_file=s'    => \$config,
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
    logging_file   => $logging,
    composites_dir => $composites,
    run_user       => $username,
    run_group      => $groupname,
    config_xsd     => $config_xsd,
    composite_xsd  => $composite_xsd,
    daemonize      => !$nofork
);

$data_services->start();
