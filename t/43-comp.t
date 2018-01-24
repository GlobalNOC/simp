#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 50;

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
    node => 'a.example.net'
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
$response = $client->test1(
    node => ['b.example.net', 'c.example.net_1', 'c.example.net_2']
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



# Request 3: like request 1, but for no hosts
$response = $client->test1(
    node   => []
);

ok(defined($response), 'request 3: we got back a response');
ok(defined($response->{'error'}), 'request 3: got an error message');
ok(!defined($response->{'results'}), 'request 3: didn\'t get results in the response');



# Request 4: simple test of rates
$response = $client->test2(
    node => ['a.example.net']
);

ok(defined($response), 'request 4: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 4: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 4: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1' => {
          'input' => num(100, THRESHOLD),
          'output' => num(20, THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          'input' => num(50, THRESHOLD),
          'output' => num(2, THRESHOLD),
          'time' => 100124,
        },
      },
    },
    'request 4: got the correct results'
);



# Request 5: like request 4, but for multiple hosts
$response = $client->test2(
    node => ['a.example.net', 'c.example.net_1']
);

ok(defined($response), 'request 5: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 5: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 5: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1' => {
          'input' => num(100, THRESHOLD),
          'output' => num(20, THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          'input' => num(50, THRESHOLD),
          'output' => num(2, THRESHOLD),
          'time' => 100124,
        },
      },
      'c.example.net_1' => {
        '1' => {
          'input' => num(100, THRESHOLD),
          'output' => num(0, THRESHOLD),
          'time' => 100131,
        },
      },
    },
    'request 5: got the correct results'
);



# Request 6: combined gauge values and rates
$response = $client->test3(
    node => 'b.example.net'
);

ok(defined($response), 'request 6: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 6: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 6: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'b.example.net' => {
        '1' => {
          '*name' => 'eth1',
          'status' => 1,
          'inPackets' => num(0.1, THRESHOLD),
          'outPackets' => num(1, THRESHOLD),
          'time' => 100132,
        },
        '3' => {
          '*name' => 'eth2',
          'status' => 1,
          'inPackets' => num(2, THRESHOLD),
          'outPackets' => num(0.1, THRESHOLD),
          'time' => 100132,
        },
      },
    },
    'request 6: got the correct results'
);



# Request 7: test of not including optional input parameter, as well as of "regexp" and "replace"
$response = $client->test4(
    node => ['a.example.net', 'c.example.net_1', 'c.example.net_2'] # we expect nothing for c.example.net_1
);

ok(defined($response), 'request 7: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 7: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 7: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1.1' => {
          'name' => 'CPU1/1',
          'nameMod1' => 'U1/1',
          'nameMod2' => 'CPU1/1',
          'time' => 100135,
        },
        '1.2' => {
          'name' => 'CPU1/2',
          'nameMod1' => 'U1/2',
          'nameMod2' => 'CPU1/TWO',
          'time' => 100135,
        },
        '2.3' => {
          'name' => 'CPU2/3',
          'nameMod1' => 'U2/3',
          'nameMod2' => 'CPU2/3',
          'time' => 100135,
        },
      },
      'c.example.net_2' => {
        '1' => {
          'name' => 'CPU1',
          'nameMod1' => 'U1',
          'nameMod2' => 'CPU1',
          'time' => 100100,
        },
        '2' => {
          'name' => 'CPU2',
          'nameMod1' => 'U2',
          'nameMod2' => 'CPUTWO',
          'time' => 100100,
        },
      },
    },
    'request 7: got the correct results'
);

# Request 8: test of including an optional input parameter
$response = $client->test4(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    cpuName => ['CPU2'],
);

ok(defined($response), 'request 8: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 8: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 8: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'b.example.net' => {
        '2' => {
          'name' => 'CPU2',
          'nameMod1' => 'U2',
          'nameMod2' => 'CPUTWO',
          'time' => 100110,
        },
      },
      'c.example.net_2' => {
        '2' => {
          'name' => 'CPU2',
          'nameMod1' => 'U2',
          'nameMod2' => 'CPUTWO',
          'time' => 100100,
        },
      },
    },
    'request 8: got the correct results'
);



# Request 9: test of including an optional input parameter, when no hosts have a match
$response = $client->test4(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    cpuName => ['CPU9001', 'CPU9002'],
);

ok(defined($response), 'request 9: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 9: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 9: got results in the response');

cmp_deeply(
    $response->{'results'},
    { },
    'request 9: got the correct results'
);



# Request 10: test of including a required input parameter
$response = $client->test5(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    cpuName => ['CPU1/1', 'CPU1', 'CPU2', 'CPU9001'],
);

ok(defined($response), 'request 10: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 10: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 10: got results in the response');

cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1.1' => {
          'name' => 'CPU1/1',
          'usagePct' => 6,
          'time' => 100135,
        },
      },
      'b.example.net' => {
        '2' => {
          'name' => 'CPU2',
          'usagePct' => 15,
          'time' => 100110,
        },
      },
      'c.example.net_2' => {
        '1' => {
          'name' => 'CPU1',
          'usagePct' => 4,
          'time' => 100100,
        },
        '2' => {
          'name' => 'CPU2',
          'usagePct' => 6,
          'time' => 100100,
        },
      },
    },
    'request 10: got the correct results'
);



# Request 11: test of *not* including a required input parameter
$response = $client->test5(
    node    => ['a.example.net', 'b.example.net'],
);

ok(defined($response), 'request 11: we got back a response');
ok(defined($response->{'error'}), 'request 11: got an error message');
ok(!defined($response->{'results'}), 'request 11: didn\'t get results in the response');



# Request 12: test of including a required input parameter, but with an empty array
$response = $client->test5(
    node    => 'a.example.net',
    cpuName => [],
);

ok(defined($response), 'request 12: we got back a response');
ok(defined($response->{'error'}), 'request 12: got an error message');
ok(!defined($response->{'results'}), 'request 12: didn\'t get results in the response');



# Request 13: elaborate test of numerical <fctn>s, multiple <fctn>s per value
$response = $client->test6(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_1'],
);

ok(defined($response), 'request 13: we got back a response');
ok(!defined($response->{'error'}) && !defined($response->{'error_text'}),
   'request 13: didn\'t get an error message');
ok(defined($response->{'results'}), 'request 13: got results in the response');

# "# ***" means "take particular note of these values
# with regards to the <fctn>s being tested"
cmp_deeply(
    $response->{'results'},
    {
      'a.example.net' => {
        '1' => {
          '*name' => 'eth0',
          'status' => 1,
          'statusFunky' => num(0 , THRESHOLD),
          'statusInverted' => num(0 , THRESHOLD),
          'input' => num(800, THRESHOLD),
          'outputScaledWrong' => num(2.5, THRESHOLD),
          'inPacketsTimes5Mod7' => num(0, THRESHOLD),  # ***
          'logOutPackets' => num(log(0.3), THRESHOLD),
          'testAddingUndef' => num(1+(log(0.3)/log(10)), THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          '*name' => 'eth1',
          'status' => 0,
          'statusFunky' => num(-1, THRESHOLD),   # ***
          'statusInverted' => num(1, THRESHOLD), # ***
          'input' => num(400, THRESHOLD),
          'outputScaledWrong' => num(0.25, THRESHOLD),
          'inPacketsTimes5Mod7' => num(0, THRESHOLD),
          'logOutPackets' => num(log(4), THRESHOLD),
          'testAddingUndef' => num(1+(log(4)/log(10)), THRESHOLD),
          'time' => 100124,
        },
      },
      'b.example.net' => {
        '1' => {
          '*name' => 'eth1',
          'status' => 1,
          'statusFunky' => num(0, THRESHOLD),
          'statusInverted' => num(0, THRESHOLD),
          'input' => num(80, THRESHOLD),
          'outputScaledWrong' => num(1/16, THRESHOLD),
          'inPacketsTimes5Mod7' => num(0, THRESHOLD),
          'logOutPackets' => num(0, THRESHOLD),
          'testAddingUndef' => num(1, THRESHOLD),
          'time' => 100132,
        },
        '3' => {
          '*name' => 'eth2',
          'status' => 1,
          'statusFunky' => num(0, THRESHOLD),
          'statusInverted' => num(0, THRESHOLD),
          'input' => num(160, THRESHOLD),
          'outputScaledWrong' => num(1/8, THRESHOLD),
          'inPacketsTimes5Mod7' => num(3, THRESHOLD),  # ***
          'logOutPackets' => num(log(0.1), THRESHOLD),
          'testAddingUndef' => num(0, THRESHOLD),
          'time' => 100132,
        },
      },
      'c.example.net_1' => {
        '1' => {
          '*name' => 'eth0',
          'status' => 1,
          'statusFunky' => num(0, THRESHOLD),
          'statusInverted' => num(0, THRESHOLD),
          'input' => num(800, THRESHOLD),
          'outputScaledWrong' => num(0, THRESHOLD),
          'inPacketsTimes5Mod7' => num(2, THRESHOLD), # ***
          'logOutPackets' => undef,    # ***
          'testAddingUndef' => undef,  # ***
          'time' => 100131,
        },
      },
    },
    'request 13: got the correct results'
);
