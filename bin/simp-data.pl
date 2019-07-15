#!/usr/bin/perl -I ../lib
use strict;
use warnings;

#use AnyEvent::Loop;
use Getopt::Long;
use GRNOC::Simp::Data;

sub usage {
    my $text = <<"EOM";
Usage: $0 [--config <file path>] [--logging <file path>] [--validation <file path>]
    [--nofork] [--user <user name>] [--group <group name>]
EOM
    print $text;
    exit( 1 );
}

use constant DEFAULT_CONFIG_FILE     => '/etc/simp/data/config.xml';
use constant DEFAULT_LOG_FILE        => '/etc/simp/data/logging.conf';
use constant DEFAULT_VALIDATION_FILE => '/etc/simp/data/validation.d/config.xsd';

my $config     = DEFAULT_CONFIG_FILE;
my $logging    = DEFAULT_LOG_FILE;
my $validation = DEFAULT_VALIDATION_FILE;
my $nofork;
my $help;
my $username;
my $groupname;

GetOptions(
    'config=s'   => \$config,
    'logging=s'  => \$logging,
    'validation' => \$validation,
    'nofork'     => \$nofork,
    'user=s'     => \$username,
    'group=s'    => \$groupname,
    'help|h|?'   => \$help
);

usage() if $help;

my $data_services = GRNOC::Simp::Data->new(
    config_file     => $config,
    logging_file    => $logging,
    validation_file => $validation,
    run_user        => $username,
    run_group       => $groupname,
    daemonize       => !$nofork 
);

$data_services->start();
