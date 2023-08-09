#!/usr/bin/perl
##--- A test script to help troubleshoot Simp Data or Simp Comp

use strict;
use warnings;

use Getopt::Long;
use Data::Dumper;
use GRNOC::RabbitMQ::Client;
use Time::HiRes qw(usleep gettimeofday tv_interval);

my $USAGE = "$0 -n|--node [node name (or CSV names for v2)]";
$USAGE .= "One of the following must also be specific\n";
$USAGE .= "  -c|--composite [composite name for Simp::Comp (or CSV names for v2)] -p|--period [time interval, default 60]\n";
$USAGE .= " OR\n";
$USAGE .= "  -o|--oid [oid string, will query simp-data]\n\n";
$USAGE .= "Additional but usually unneeded options:\n";
$USAGE .= "  -v|--version [simp version to test (1 or 2)]\n";
$USAGE .= "  --rmq_host <rabbitmq host, default 127.0.0.1>\n";
$USAGE .= "  --rmq_port <rabbitmq port, default 5672>\n";
$USAGE .= "  --rmq_user <rabbitmq user, default \"guest\">\n";
$USAGE .= "  --rmq_pass <rabbitmq password, default \"guest\">\n";
$USAGE .= "  --rmq_exchange <rabbitmq exchange, default \"Simp\">\n";
$USAGE .= "  --rmq_timeout <timeout for simp call, default 60>\n";
$USAGE .= "  --debug <turn on debugging, default 0>\n";
$USAGE .= "  --help <show this message>\n";

my $composite;
my $oid;
my $node;
my $period       = 60;
my $version      = 2;
my $rmq_host     = "127.0.0.1";
my $rmq_port     =  5672;
my $rmq_user     = "guest";
my $rmq_pass     = "guest";
my $rmq_exchange = "Simp";
my $rmq_timeout  = 60;
my $debug        = 0;
my $help;

GetOptions(
    "c|composite=s" => \$composite,
    "o|oid=s"       => \$oid,
    "n|node=s"      => \$node,
    "v|version=i"   => \$version,
    "p|period=i"    => \$period,
    "rmq_host=s"    => \$rmq_host,
    "rmq_port=s"    => \$rmq_port,
    "rmq_user=s"    => \$rmq_user,
    "rmq_pass=s"    => \$rmq_pass,
    "rmq_timeout=i" => \$rmq_timeout,
    "debug"         => \$debug,
    "h|help"        => \$help
) or die $USAGE;

die $USAGE if ($help);

if (! $composite && ! $oid){
    warn "Neither composite or OID specific, please specify one.";
    die $USAGE;
}
if ($composite && $oid){
    warn "Both compositive and OID specified, must only be one.";
    die $USAGE;
}

my $topic = $composite ? "Simp.Comp" : "Simp.Data";

my %rmq_args = (
    host => $rmq_host,
    port => $rmq_port,
    user => $rmq_user,
    pass => $rmq_pass,
    exchange => $rmq_exchange,
    timeout => $rmq_timeout,
    debug => $debug,
    topic => $topic
);

print "RMQ Args = " . Dumper(\%rmq_args) if ($debug);

my $client = GRNOC::RabbitMQ::Client->new(%rmq_args);

# Get any CSV for nodes or composites (Simp version 2)
my @nodes      = split(/ ?, ?/, $node);
my @composites = split(/ ?, ?/, $composite);

# Track the runtime of the test
my $start = [gettimeofday];

my $result;

# Test Simp::Data
if ($topic =~ /Data/){

    print("testing Simp::Data for OID $oid on node $node\n") if ($debug);

    $result = $client->get(node => [$node], oidmatch => $oid );
}
# Test Simp::Comp
else {

    if ($version == 1) {
        print("Testing Simp::Comp (v1) for composite $composite on node $node\n") if ($debug);
        $result = $client->$composite(node => $node, period => $period);
    }
    else {
        print("Testing Simp::Comp (v2) for composite $composite on node $node\n") if ($debug);

        my $requests;

        # Multi-Node
        if (scalar(@nodes) > 1) {
            $requests = [];

            for my $n (@nodes) {

                # Multi-Composite
                if (scalar(@composites) > 1) {
                    for my $c (@composites) {
                        my $request = {
                            node      => $n,
                            composite => $c,
                            interval  => $period
                        };
                        push(@$requests, $request);
                    }
                }
                # Single Composite
                else {
                    my $request = {
                        node      => $n,
                        composite => $composite,
                        interval  => $period
                    };
                    push(@$requests, $request);
                }
            }
        }
        # Multi-Composite
        elsif (scalar(@composites) > 1) {
            $requests = [];

            for my $c (@composites) {
                my $request = {
                    node      => $node,
                    composite => $c,
                    interval  => $period
                };
                push(@$requests, $request);
            }
        }
        # Single node and composite
        else {
            $requests = {
                node      => $node,
                composite => $composite,
                interval  => $period
            };
        }

        $result = $client->get(requests => $requests);
    }
}

my $end = [gettimeofday];
my $duration = tv_interval($start, $end);

print Dumper($result);
print "Took $duration seconds\n";
