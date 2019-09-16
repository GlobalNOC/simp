#!/usr/bin/perl

use strict;
use warnings;

# test client to help understand the functionality
# and performance of the Simp.Data rabbit service

use AnyEvent;
use Data::Dumper;
use Time::HiRes qw(usleep gettimeofday tv_interval);

use GRNOC::Log;
use GRNOC::RabbitMQ::Client;

my $client = GRNOC::RabbitMQ::Client->new(
    host     => "127.0.0.1",
    port     => 5672,
    user     => "guest",
    pass     => "guest",
    exchange => 'Simp',
    timeout  => 15,
    topic    => 'Simp.CompData'
);

my $results;

# create and store logger object
#my $grnoc_log = GRNOC::Log->new( config => "./poo" );
#my $logger = GRNOC::Log->get_logger();

#print Dumper($client->help());
#print Dumper($client->ping());

#print "getInterfaceGbps:\n";
#print Dumper($client->interfaceGbps(ipaddrs => '156.56.6.103', ifName =>'fxp0.0',));
#exit;
#while(1){
#print time() ."\n";
#$client->interfaceGbps(ipaddrs => '156.56.6.103');
#}

#warn Dumper($client->CPU(ipaddrs => '156.56.6.103'));
#warn Dumper($client->Temp(ipaddrs => '156.56.6.103'));
while (1)
{
    my $start = time;

    #print time()."\n";
    #$client->ping();
    #$client->CPU(ipaddrs => '156.56.6.103');
    my $x = 500;
    for (my $y = 0; $y < $x; $y++)
    {
        my $res =
          $client->interfaceGbps(ipaddrs => '156.56.6.103', ifName => 'fxp0.0');
    }
    my $end = time;
    my $et  = $end - $start;

    if ($et == 0)
    {
        warn "huh\n";
        next;
    }
    my $rate = $x / ($et * 1.0);

    warn "query rate: $rate calls / sec\n";
}
