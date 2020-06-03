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
Usage: $0 [--composite <composite_name>]  [--hosts <host names comma-separated] [--iter <iterations>]
EOM
    print $text;
    exit( 1 );
}

my $composite;
my $hosts;
my $iter;
my $help;
GetOptions(
    'composite|c=s' => \$composite,
    'hosts=s'       => \$hosts,
    'iter|i=s'      => \$iter,
    'help|h|?'      => \$help
) or usage();

usage() if $help;

# RabbitMQ Client Setup
my $client = GRNOC::RabbitMQ::Client->new(
    host     => "RabbitMQ HOST IP",
    port     => 5672,
    user     => "RabbitMQ USERNAME",
    pass     => "RabbitMQ PASSWORD",
    exchange => 'Simp',
    timeout  => 60,
    debug    => 1,
    topic    => 'Simp.Comp'
);

# An array of hosts to request data for
my @nodes = (
    'hostname1',
    'hostname2'
);

# Continue to request data from Simp.Comp on "interval" seconds
my $timer = AnyEvent->timer(
    interval => 30,
    cb       => sub { get(\@nodes); }
);

AnyEvent->condvar->recv;

sub get {

    my $nodes = shift;

    # Request data for each node
    foreach my $node (@{$nodes}) {

        # Send RabbitMQ request for Simp.Comp to compute data for COMPOSITE_NAME
        my $data = $client->COMPOSITE_NAME(
            node           => $node,
            period         => 30,
            async_callback => sub { warn Dumper(shift); }
        );
    }
}
