#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 19;

use GRNOC::RabbitMQ::Client;
use Test::Deep qw(cmp_deeply num);

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

cmp_deeply(
    $results->{'results'},
    {
      'a.example.net' => {
        '1.3.6.1.2.1.31.1.1.1.1.1' => { 'value' => 'eth0', 'time' => 100124 },
        '1.3.6.1.2.1.31.1.1.1.1.2' => { 'value' => 'eth1', 'time' => 100124 },
      },
    },
    'request 1: we got the correct data in the response'
);



# A simple request involving rates:
$results = $client->get_rate(
    node     => ['c.example.net_1'],
    oidmatch => ['1.3.6.1.2.1.2.2.1.11.*'],
);

ok(defined($results), 'request 2: we got back a response');
ok(!defined($results->{'error'}), 'request 2: we didn\'t get an error message');
ok(defined($results->{'results'}), 'request 2: we got results in the response');

cmp_deeply(
    $results->{'results'},
    {
      'c.example.net_1' => {
        '1.3.6.1.2.1.2.2.1.11.1' => { 'value' => num(0.5, 1e-6), 'time' => 100131 },
      },
    },
    'request 2: we got the correct data in the response'
);



# A simple request involving host variables:
$results = $client->get(
    node     => ['c.example.net_1'],
    oidmatch => ['vars.*'],
);

ok(defined($results), 'request 3: we got back a response');
ok(!defined($results->{'error'}), 'request 3: we didn\'t get an error message');
ok(defined($results->{'results'}), 'request 3: we got results in the response');

cmp_deeply(
    $results->{'results'},
    {
      'c.example.net_1' => {
        'vars.test'    => { 'value' => 'bye', 'time' => 100131 },
        'vars.card_no' => { 'value' => '1',   'time' => 100131 },
      },
    },
    'request 3: we got the correct data in the response'
);



# A request that leaves out a required field, part 1:
$results = $client->get(
    node     => ['c.example.net_1'],
);

ok(defined($results), 'request 4: we got back a response');
ok(defined($results->{'error'}), 'request 4: we got an error message');
ok(!defined($results->{'results'}), 'request 4: we didn\'t get results');



# A request that leaves out a required field, part 2:
$results = $client->get(
    oidmatch => ['1.3.6.1.2.1.2.2.1.11.*'],
);

ok(defined($results), 'request 5: we got back a response');
ok(defined($results->{'error'}), 'request 5: we got an error message');
ok(!defined($results->{'results'}), 'request 5: we didn\'t get results');
