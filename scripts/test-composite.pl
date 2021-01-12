#!/usr/bin/perl

use strict;
use warnings;

use Time::HiRes qw(usleep gettimeofday tv_interval);
use Data::Dumper;
use GRNOC::RabbitMQ::Client;
use AnyEvent;
use Getopt::Long;

# Command line usage and parameters
sub usage {
    my $text = <<"EOM";
Usage: $0 [--composite|-c <composite_name>]  [--nodes|-n <Node Name(s) Comma-Separated] [--user|-u <RabbitMQ User>] [--pass|-p <RabbitMQ Password>]
EOM
    print $text;
    exit(1);
}

my $composite;
my $nodecsv;
my $user;
my $pass;
my $help;
GetOptions(
    'composite|c=s' => \$composite,
    'nodes|n=s'     => \$nodecsv,
    'user|u=s'      => \$user,
    'pass|p=s'      => \$pass,
    'help|h|?'      => \$help
) or usage();

usage() if $help;

# Name of the composite we want
warn('No composite was specified!') and usage() unless ($composite);

# An array of hosts to request data for
my @nodes = split(/,/, $nodecsv);
warn('No host(s) were specified!') and usage() unless (@nodes);

# Credentials for RabbitMQ
warn('RabbitMQ Username and Password must be specified!') and usage() unless ($user && $pass);

# RabbitMQ Client Setup
my $client = GRNOC::RabbitMQ::Client->new(
    host     => "127.0.0.1",
    port     => 5672,
    user     => $user,
    pass     => $pass,
    exchange => 'Simp',
    timeout  => 60,
    debug    => 1,
    topic    => 'Simp.Comp'
);

# Continue to request data from Simp.Comp on "interval" seconds
my $timer = AnyEvent->timer(
    interval => 30,
    cb       => sub { get(); }
);

AnyEvent->condvar->recv;

sub get {

    # Request data for each node
    foreach my $node (@nodes) {

        # Send RabbitMQ request for Simp.Comp to compute data for COMPOSITE_NAME
        my $data = $client->$composite(
            node           => $node,
            period         => 30,
            async_callback => sub { warn Dumper(shift); }
        );
    }
}
