#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 17;

use GRNOC::RabbitMQ::Client;
use Test::Deep qw(cmp_deeply num);

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



# Request 1: get multiple oidmatches for a node
my $response = $client->get(
    node =>     ['a.example.net'],
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.1.*', '1.3.6.1.2.1.2.2.1.8.*'],
);

ok(defined($response), 'request 1: got a response');
ok(!defined($response->{'error'}), 'request 1: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 1: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.1.2' => { 'value' => 'eth1', 'time' => 100124 },
        '1.3.6.1.2.1.2.2.1.8.1'    => { 'value' => 1,      'time' => 100124 },
        '1.3.6.1.2.1.2.2.1.8.2'    => { 'value' => 0,      'time' => 100124 },
      },
    },
    'request 1: got the correct results in the response');



# Request 2: get data using an oidmatch that covers multiple collected trees
$response = $client->get(
    node     => ['c.example.net_2'],
    oidmatch => ['2.10.9994.*'],
);

ok(defined($response), 'request 2: got a response');
ok(!defined($response->{'error'}), 'request 2: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 2: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'c.example.net_2' => {
        '2.10.9994.27.1.1' => { 'value' => 'CPU1', 'time' => 100100 },
        '2.10.9994.27.1.2' => { 'value' => 'CPU2', 'time' => 100100 },
        '2.10.9994.27.8.1' => { 'value' => 4,      'time' => 100100 },
        '2.10.9994.27.8.2' => { 'value' => 6,      'time' => 100100 },
      },
    },
    'request 2: got the correct results (including all OIDs) in the response'
);



# Request 3: get multiple oidmatches for a node, counter edition
$response = $client->get_rate(
    node =>     ['b.example.net'],
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.6.*', '1.3.6.1.2.1.2.2.1.11.*'],
);

ok(defined($response), 'request 3: got a response');
ok(!defined($response->{'error'}), 'request 3: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 3: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'b.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.6.1' => { 'value' => num(10, THRESHOLD),  'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.6.3' => { 'value' => num(20, THRESHOLD),  'time' => 100132 },
        '1.3.6.1.2.1.2.2.1.11.1'   => { 'value' => num(0.1, THRESHOLD), 'time' => 100132 },
        '1.3.6.1.2.1.2.2.1.11.3'   => { 'value' => num(2, THRESHOLD),   'time' => 100132 },
      },
    },
    'request 3: got the correct results in the response'
);



# Request 4: get gauge data from multiple hosts
my $all_hosts = ['a.example.net', 'b.example.net', 'c.example.net_1', 'c.example.net_2'];
$response = $client->get(
    node     => $all_hosts,
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.1.*'],
);

ok(defined($response), 'request 4: got a response');
ok(!defined($response->{'error'}), 'request 4: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 4: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.1.2' => { 'value' => 'eth1', 'time' => 100124 },
      },
      'b.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth1', 'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.1.3' => { 'value' => 'eth2', 'time' => 100132 },
      },
      'c.example.net_1' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100131 },
      },
      'c.example.net_2' => {
        '1.3.6.1.2.1.31.1.1.1.1.8' => { 'value' => 'eth7', 'time' => 100135 },
      },
    },
    'request 4: got the correct results in the response'
);
