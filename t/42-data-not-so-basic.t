#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 5;

use Test::Deep qw(cmp_deeply num);
use GRNOC::RabbitMQ::Client;

use constant THRESHOLD => 1e-9;

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

# Get multiple subtrees for a node
my $response = $client->get(
    node =>     ['a.example.net'],
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.1.*', '1.3.6.1.2.1.2.2.1.8.*'],
);

ok(defined($response), 'request 1: got a response');
ok(!defined($response->{'error'}), 'request 1: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 1: got results in the response');

cmp_deeply($response->{'results'},
    {
      'a.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.1.2' => { 'value' => 'eth1', 'time' => 100124 },
        '1.3.6.1.2.1.2.2.1.8.1' =>    { 'value' => 1,      'time' => 100124 },
        '1.3.6.1.2.1.2.2.1.8.2' =>    { 'value' => 0,      'time' => 100124 },
      }
    },
    'request 1: got the correct response');
