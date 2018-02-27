#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 37;

use GRNOC::RabbitMQ::Client;
use Test::Deep qw(cmp_deeply num);
use Data::Dumper;
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
        '1.3.6.1.2.1.31.1.1.1.6.1' => { 'value' => num(5.83333333333333, THRESHOLD),  'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.6.3' => { 'value' => num(10.1666666666667, THRESHOLD),  'time' => 100132 },
        '1.3.6.1.2.1.2.2.1.11.1'   => { 'value' => num(0.075, THRESHOLD), 'time' => 100132 },
        '1.3.6.1.2.1.2.2.1.11.3'   => { 'value' => num(1.00833333333333, THRESHOLD),   'time' => 100132 },
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



# Request 5: get counter data from multiple hosts,
# at multiple cadences, with multiple oidmatches
$response = $client->get_rate(
    node     => ['a.example.net', 'b.example.net', 'c.example.net_1'],
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.6.*', '1.3.6.1.2.1.31.1.1.1.10.*'],
);

ok(defined($response), 'request 5: got a response');
ok(!defined($response->{'error'}), 'request 5: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 5: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.6.1'  => { 'value' => num(51.2521008403361, THRESHOLD), 'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.6.2'  => { 'value' => num(25.2100840336134, THRESHOLD),  'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.10.1' => { 'value' => num(10.6050420168067, THRESHOLD),  'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.10.2' => { 'value' => num(1.00840336134454, THRESHOLD),   'time' => 100124 },
      },
      'b.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.6.1'  => { 'value' => num(5.83333333333333, THRESHOLD),  'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.6.3'  => { 'value' => num(10.1666666666667, THRESHOLD),  'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.10.1' => { 'value' => num(0.333333333333333, THRESHOLD), 'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.10.3' => { 'value' => num(0.75, THRESHOLD),   'time' => 100132 },
      },
      'c.example.net_1' => {
        '1.3.6.1.2.1.31.1.1.1.6.1'  => { 'value' => num(45.045045045045, THRESHOLD), 'time' => 100131 },
        '1.3.6.1.2.1.31.1.1.1.10.1' => { 'value' => num(0, THRESHOLD),   'time' => 100131 },
      },
    },
    'request 5: got the correct results in the response'
);



# Request 6: trying to get counter data when we only have an OID for one epoch
$response = $client->get_rate(
    node     => ['c.example.net_2'],
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.6.*'],
);

ok(defined($response), 'request 6: got a response');
ok(!defined($response->{'error'}), 'request 6: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 6: got results in the response');

cmp_deeply(
    $response->{'results'},
    { },
    'request 6: got the correct results in the response (namely, no data)'
);



# Request 7: get data, where trees on different hosts have different depths
# and some hosts don't have data at all
$response = $client->get(
    node     => $all_hosts,
    oidmatch => ['2.10.9994.27.8.*'],
);

ok(defined($response), 'request 7: got a response');
ok(!defined($response->{'error'}), 'request 7: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 7: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '2.10.9994.27.8.1.1' => { 'value' => 6,  'time' => 100135 },
        '2.10.9994.27.8.1.2' => { 'value' => 2,  'time' => 100135 },
        '2.10.9994.27.8.2.3' => { 'value' => 71, 'time' => 100135 },
      },
      'b.example.net' => {
        '2.10.9994.27.8.2' => { 'value' => 15, 'time' => 100110 },
      },
      'c.example.net_2' => {
        '2.10.9994.27.8.1' => { 'value' => 4, 'time' => 100100 },
        '2.10.9994.27.8.2' => { 'value' => 6, 'time' => 100100 },
      },
    },
    'request 7: got the correct results in the response'
);



# Request 8: get host variables for multiple hosts
$response = $client->get(
    node     => $all_hosts,
    oidmatch => ['vars.*'],
);

ok(defined($response), 'request 8: got a response');
ok(!defined($response->{'error'}), 'request 8: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 8: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        'vars.test' => { 'value' => 'hi', 'time' => 100124 },
      },
      'c.example.net_1' => {
        'vars.test'    => { 'value' => 'bye', 'time' => 100131 },
        'vars.card_no' => { 'value' => '1',   'time' => 100131 },
      },
      'c.example.net_2' => {
        'vars.test'    => { 'value' => 'lol', 'time' => 100135 },
        'vars.card_no' => { 'value' => '2',   'time' => 100135 },
      },
    },
    'request 8: got the correct results in the response'
);



# Request 9: get data from multiple collection trees, and for multiple hosts
$response = $client->get(
    node     => $all_hosts,
    oidmatch => ['1.3.6.1.2.1.31.1.1.1.1.*', '2.10.9994.27.1.*'],
);

ok(defined($response), 'request 9: got a response');
ok(!defined($response->{'error'}), 'request 9: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 9: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.1.2' => { 'value' => 'eth1', 'time' => 100124 },
        '2.10.9994.27.1.1.1' => { 'value' => 'CPU1/1', 'time' => 100135 },
        '2.10.9994.27.1.1.2' => { 'value' => 'CPU1/2', 'time' => 100135 },
        '2.10.9994.27.1.2.3' => { 'value' => 'CPU2/3', 'time' => 100135 },
      },
      'b.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth1', 'time' => 100132 },
        '1.3.6.1.2.1.31.1.1.1.1.3' => { 'value' => 'eth2', 'time' => 100132 },
        '2.10.9994.27.1.2' => { 'value' => 'CPU2', 'time' => 100110 },
      },
      'c.example.net_1' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100131 },
      },
      'c.example.net_2' => {
        '1.3.6.1.2.1.31.1.1.1.1.8' => { 'value' => 'eth7', 'time' => 100135 },
        '2.10.9994.27.1.1' => { 'value' => 'CPU1', 'time' => 100100 },
        '2.10.9994.27.1.2' => { 'value' => 'CPU2', 'time' => 100100 },
      },
    },
    'request 9: got the correct results in the response'
);
