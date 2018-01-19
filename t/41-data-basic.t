#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 9;

use List::Util qw(all);
use GRNOC::RabbitMQ::Client;

my $client = GRNOC::RabbitMQ::Client->new(
    host     => '127.0.0.1',
    port     => 5673,
    user     => 'guest',
    pass     => 'guest',
    timeout  => 3,
    exchange => 'Simp',
    topic    => 'Simp.Data',
);

ok(defined($client), 'RabbitMQ client could be constructed');



# A very simple request:
my $results = $client->get(
    node     => ['a.example.net'],
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.1.*'],
);

ok(defined($results), 'request 1: we got back a response');
ok(!defined($results->{'error'}), 'request 1: we didn\'t get an error message');
ok(defined($results->{'results'}), 'request 1: we got results in the response');

my $res = $results->{'results'};

ok(defined($res->{'a.example.net'}), 'request 1: got results for a.example.net');
ok(scalar(keys %$res) == 1, 'request 1: got *only* results for a.example.net');

$res = $res->{'a.example.net'};

ok(scalar(keys %$res) == 2 &&
     defined($res->{'1.3.6.1.2.1.31.1.1.1.1.1'}) &&
     defined($res->{'1.3.6.1.2.1.31.1.1.1.1.2'}),
   'request 1: right OIDs are returned');

ok(($res->{'1.3.6.1.2.1.31.1.1.1.1.1'}{'value'} eq 'eth0') &&
     ($res->{'1.3.6.1.2.1.31.1.1.1.1.2'}{'value'} eq 'eth1'),
   'request 1: right values for the OIDs are returned');

ok((all { ($res->{$_}{'time'}) == 100124 } (keys %$res)),
   'request 1: got the most recent values of the OIDs');
