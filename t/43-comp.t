#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 9;

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
    topic    => 'Simp.CompData',
);

ok(defined($client), 'RabbitMQ client could be constructed');



# Request 1: simple test of scans and unprocessed gauge values
my $response = $client->test1(
    node   => 'a.example.net'
);

ok(defined($response), 'request 1: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 1: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 1: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1' => {
          'name' => 'eth0',
          'outRawCounter' => 1299,
          'time' => 100124,
        },
        '2' => {
          'name' => 'eth1',
          'outRawCounter' => 9000120,
          'time' => 100124,
        },
      },
    },
    'request 1: got the correct results'
);



# Request 2: like request 1, but for multiple hosts
my $response = $client->test1(
    node   => ['b.example.net', 'c.example.net_1', 'c.example.net_2']
);

ok(defined($response), 'request 2: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 2: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 2: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'b.example.net' => {
        '1' => {
          'name' => 'eth1',
          'outRawCounter' => 200041,
          'time' => 100132,
        },
        '3' => {
          'name' => 'eth2',
          'outRawCounter' => 92,
          'time' => 100132,
        },
      },
      'c.example.net_1' => {
        '1' => {
          'name' => 'eth0',
          'outRawCounter' => 1000,
          'time' => 100131,
        },
      },
      'c.example.net_2' => {
        '8' => {
          'name' => 'eth7',
          'outRawCounter' => 6758799,
          'time' => 100135,
        },
      },
    },
    'request 2: got the correct results'
);
