#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 90;

use GRNOC::RabbitMQ::Client;
use Test::Deep qw(cmp_deeply num any code);
use Data::Dumper;

use constant THRESHOLD => 1e-9;

sub check_response {
    my ($request_num, $actual_response, $expected_results) = @_;

    ok(defined($actual_response), "request $request_num: we got back a response");
    ok(!defined($actual_response->{'error'}) && !defined($actual_response->{'error_text'}),
       "request $request_num: didn't get an error message");
    ok(defined($actual_response->{'results'}), "request $request_num: got results in the response");

    cmp_deeply($actual_response->{'results'}, $expected_results,
               "request $request_num: got the correct results");
}

sub error_expected {
    my ($request_num, $response) = @_;

    ok(defined($response), "request $request_num: we got back a response");
    ok(defined($response->{'error'}), "request $request_num: got an error response");
    ok(!defined($response->{'results'}), "request $request_num: didn't get results in the response");
}

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

check_response(1, $response,
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
    }
);



# Request 2: like request 1, but for multiple hosts
$response = $client->test1(
    node => ['b.example.net', 'c.example.net_1', 'c.example.net_2']
);

check_response(2, $response,
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
    }
);



# Request 3: like request 1, but for no hosts
$response = $client->test1(
    node => []
);

error_expected(3, $response);



# Request 4: simple test of rates
$response = $client->test2(
    node => ['a.example.net']
);

check_response(4, $response,
    {
      'a.example.net' => {
        '1' => {
          'input' => num(51.2521008403361, THRESHOLD),
          'output' => num(10.6050420168067, THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          'input' => num(25.2100840336134, THRESHOLD),
          'output' => num(1.00840336134454, THRESHOLD),
          'time' => 100124,
        },
      },
    }
);



# Request 5: like request 4, but for multiple hosts
$response = $client->test2(
    node => ['a.example.net', 'c.example.net_1']
);

check_response(5, $response,
    {
      'a.example.net' => {
        '1' => {
          'input' => num(51.2521008403361, THRESHOLD),
          'output' => num(10.6050420168067, THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          'input' => num(25.2100840336134, THRESHOLD),
          'output' => num(1.00840336134454, THRESHOLD),
          'time' => 100124,
        },
      },
      'c.example.net_1' => {
        '1' => {
          'input' => num(45.045045045045, THRESHOLD),
          'output' => num(0, THRESHOLD),
          'time' => 100131,
        },
      },
    }
);



# Request 6: combined gauge values and rates
$response = $client->test3(
    node => 'b.example.net'
);

check_response(6, $response,
    {
      'b.example.net' => {
        '1' => {
          '*name' => 'eth1',
          'status' => 1,
          'inPackets' => num(0.075, THRESHOLD),
          'outPackets' => num(0.508333333333333, THRESHOLD),
          'time' => 100132,
        },
        '3' => {
          '*name' => 'eth2',
          'status' => 1,
          'inPackets' => num(1.00833333333333, THRESHOLD),
          'outPackets' => num(0.0583333333333333, THRESHOLD),
          'time' => 100132,
        },
      },
    }
);



# Request 7: test of not including optional input parameter, as well as of "regexp" and "replace"
$response = $client->test4(
    node => ['a.example.net', 'c.example.net_1', 'c.example.net_2'] # we expect nothing for c.example.net_1
);

check_response(7, $response,
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
    }
);

# Request 8: test of including an optional input parameter
$response = $client->test4(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    cpuName => ['^CPU2$'],
);


check_response(8, $response,
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
    }
);



# Request 9: test of including an optional input parameter, when no hosts have a match
$response = $client->test4(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    cpuName => ['CPU9001', 'CPU9002'],
);

check_response(9, $response,
    { }
);



# Request 10: test of including a required input parameter
$response = $client->test5(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    cpuName => ['CPU1/1', 'CPU1$', 'CPU2$', 'CPU9001'],
);

check_response(10, $response,
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
    }
);



# Request 11: test of *not* including a required input parameter
$response = $client->test5(
    node => ['a.example.net', 'b.example.net'],
);

error_expected(11, $response);



# Request 12: test of including a required input parameter, but with an empty array
$response = $client->test5(
    node    => 'a.example.net',
    cpuName => [],
);

error_expected(12, $response);



# Request 13: elaborate test of numerical <fctn>s, multiple <fctn>s per value
$response = $client->test6(
    node => ['a.example.net', 'b.example.net', 'c.example.net_1'],
);

# "# ***" means "take particular note of these values
# with regards to the <fctn>s being tested"
check_response(13, $response,
    {
      'a.example.net' => {
        '1' => {
          '*name' => 'eth0',
          'status' => 1,
          'statusFunky' => num(0 , THRESHOLD),
          'statusInverted' => num(0 , THRESHOLD),
          'input' => num(410.016806722689, THRESHOLD),
          'outputScaledWrong' => num(1.32563025210084, THRESHOLD),
          'inPacketsTimes5Mod7' => num(0, THRESHOLD),  # ***
          'logOutPackets' => num(-1.83468451394509, THRESHOLD),
          'testAddingUndef' => num(0.203206639560297, THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          '*name' => 'eth1',
          'status' => 0,
          'statusFunky' => num(-1, THRESHOLD),   # ***
          'statusInverted' => num(1, THRESHOLD), # ***
          'input' => num(201.680672268907, THRESHOLD),
          'outputScaledWrong' => num(0.126050420168068, THRESHOLD),
          'inPacketsTimes5Mod7' => num(0, THRESHOLD),
          'logOutPackets' => num(0.701515430230464, THRESHOLD),
          'testAddingUndef' => num(1.30466428031908, THRESHOLD),
          'time' => 100124,
        },
      },
      'b.example.net' => {
        '1' => {
          '*name' => 'eth1',
          'status' => 1,
          'statusFunky' => num(0, THRESHOLD),
          'statusInverted' => num(0, THRESHOLD),
          'input' => num(46.6666666666666, THRESHOLD),
          'outputScaledWrong' => num(0.0416666666666666, THRESHOLD),
          'inPacketsTimes5Mod7' => num(0, THRESHOLD),
          'logOutPackets' => num(-0.676617878608735, THRESHOLD),
          'testAddingUndef' => num(0.706148588963142, THRESHOLD),
          'time' => 100132,
        },
        '3' => {
          '*name' => 'eth2',
          'status' => 1,
          'statusFunky' => num(0, THRESHOLD),
          'statusInverted' => num(0, THRESHOLD),
          'input' => num(81.3333333333336, THRESHOLD),
          'outputScaledWrong' => num(0.09375, THRESHOLD),
          'inPacketsTimes5Mod7' => num(5, THRESHOLD),  # ***
          'logOutPackets' => num(-2.84158159372673, THRESHOLD),
          'testAddingUndef' => num(-0.234083206033368, THRESHOLD),
          'time' => 100132,
        },
      },
      'c.example.net_1' => {
        '1' => {
          '*name' => 'eth0',
          'status' => 1,
          'statusFunky' => num(0, THRESHOLD),
          'statusInverted' => num(0, THRESHOLD),
          'input' => num(360.36036036036, THRESHOLD),
          'outputScaledWrong' => num(0, THRESHOLD),
          'inPacketsTimes5Mod7' => num(1, THRESHOLD), # ***
          'logOutPackets' => undef,    # ***
          'testAddingUndef' => undef,  # ***
          'time' => 100131,
        },
      },
    }
);



# Request 14: test of using two <input>s, where we specify both of them
$response = $client->test7(
    node    => ['a.example.net', 'b.example.net', 'c.example.net_2'],
    ifName  => ['eth1', 'eth2'],
    cpuName => ['^CPU2$'],
);


check_response(14, $response,
    {
      'a.example.net' => {
        '2' => {
          '*ifName' => 'eth1',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(25.2100840336134, THRESHOLD),
          'time' => 100124,
        },
      },
      'b.example.net' => {
        '1' => {
          '*ifName' => 'eth1',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(5.83333333333333, THRESHOLD),
          'time' => 100132,
        },
        '3' => {
          '*ifName' => 'eth2',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(10.1666666666667, THRESHOLD),
          'time' => 100132,
        },
        '2' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU2',
          'usageAsFraction' => num(0.15, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100110,
        },
      },
      'c.example.net_2' => {
        '2' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU2',
          'usageAsFraction' => num(0.06, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100100,
        },
      },
    }
);



# Request 15: test of two <input>s, where only one is specified
$response = $client->test7(
    node   => ['a.example.net', 'b.example.net', 'c.example.net_1'],
    ifName => ['eth2'],
);

check_response(15, $response,
    {
      'a.example.net' => {
        '1.1' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU1/1',
          'usageAsFraction' => num(0.06, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100135,
        },
        '1.2' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU1/2',
          'usageAsFraction' => num(0.02, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100135,
        },
        '2.3' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU2/3',
          'usageAsFraction' => num(0.71, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100135,
        },
      },
      'b.example.net' => {
        '3' => {
          '*ifName' => 'eth2',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(10.1666666666667, THRESHOLD),
          'time' => 100132,
        },
        '2' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU2',
          'usageAsFraction' => num(0.15, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100110,
        },
      },
    }
);



# Request 16: test of two <input>s, where neither is specified
$response = $client->test7(
    node => 'a.example.net',
);

check_response(16, $response,
    {
      'a.example.net' => {
        '1.1' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU1/1',
          'usageAsFraction' => num(0.06, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100135,
        },
        '1.2' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU1/2',
          'usageAsFraction' => num(0.02, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100135,
        },
        '2.3' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU2/3',
          'usageAsFraction' => num(0.71, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100135,
        },
        '1' => {
          '*ifName' => 'eth0',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(51.2521008403361, THRESHOLD),
          'time' => 100124,
        },
        '2' => {
          '*ifName' => 'eth1',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(25.2100840336134, THRESHOLD),
          'time' => 100124,
        },
      },
    }
);



# Request 17: test of basic RPN calculator operation
$response = $client->test8(
    node => 'b.example.net'
);


check_response(17, $response,
    {
      'b.example.net' => {
        '1' => {
          '*name' => 'eth1',
          'modName' => 'Ethernet1',
          'input' => undef,
          'ioDiff' => num(5.5, THRESHOLD),
          'time' => 100132,
        },
        '3' => {
          '*name' => 'eth2',
          'modName' => 'Ethernet2',
          'input' => undef,
          'ioDiff' => num(9.4166666666667, THRESHOLD),
          'time' => 100132,
        },
      },
    }
);



# Request 18: test of running against a host with none of the scan OIDs
$response = $client->test4(
    node => 'c.example.net_1'
);

check_response(18, $response, { });



# Request 19: test of rate-based data on a host with only one epoch of collection
$response = $client->test3(
    node => 'c.example.net_2'
);

check_response(19, $response,
    {
      'c.example.net_2' => {
        '8' => {
          '*name' => 'eth7',
          'status' => 1,
          'inPackets' => undef,
          'outPackets' => undef,
          'time' => 100135,
        },
      },
    }
);



# Request 20: Test of a <composite> resembling something you actually might
# see in production
$response = $client->interface(
    node => ['a.example.net', 'b.example.net', 'c.example.net_1', 'c.example.net_2']
);



check_response(20, $response,
    {
      'a.example.net' => {
        '1' => {
          '*node' => 'a.example.net',
          '*intf' => 'eth0',
          'input' => num(410.016806722689, THRESHOLD),
          'output' => num(84.8403361344536, THRESHOLD),
          'inUcast' => num(0.0672268907563025, THRESHOLD),
          'outUcast' => num(0.159663865546218, THRESHOLD),
          'status' => 1,
          'time' => 100124,
        },
        '2' => {
          '*node' => 'a.example.net',
          '*intf' => 'eth1',
          'input' => num(201.680672268907, THRESHOLD),
          'output' => num(8.06722689075632, THRESHOLD),
          'inUcast' => num(0.0504201680672269, THRESHOLD),
          'outUcast' => num(2.01680672268908, THRESHOLD),
          'status' => 0,
          'time' => 100124,
        },
      },
      'b.example.net' => {
        '1' => {
          '*node' => 'b.example.net',
          '*intf' => 'eth1',
          'input' => num(46.6666666666666, THRESHOLD),
          'output' => num(2.66666666666666, THRESHOLD),
          'inUcast' => num(0.075, THRESHOLD),
          'outUcast' => num(0.508333333333333, THRESHOLD),
          'status' => 1,
          'time' => 100132,
        },
        '3' => {
          '*node' => 'b.example.net',
          '*intf' => 'eth2',
          'input' => num(81.3333333333336, THRESHOLD),
          'output' => num(6, THRESHOLD),
          'inUcast' => num(1.00833333333333, THRESHOLD),
          'outUcast' => num(0.0583333333333333, THRESHOLD),
          'status' => 1,
          'time' => 100132,
        },
      },
      'c.example.net_1' => {
        '1' => {
          '*node' => 'c.example.net',
          '*intf' => 'eth0',
          'input' => num(360.36036036036, THRESHOLD),
          'output' => num(0, THRESHOLD),
          'inUcast' => num(0.225225225225225, THRESHOLD),
          'outUcast' => num(0, THRESHOLD),
          'status' => 1,
          'time' => 100131,
        },
      },
      'c.example.net_2' => {
        '8' => {
          '*node' => 'c.example.net',
          '*intf' => 'eth7',
          'input' => undef,
          'output' => undef,
          'inUcast' => undef,
          'outUcast' => undef,
          'status' => 1,
          'time' => 100135,
        },
      },
    }
);



# Request 21: like request 20, but for a specific ifName
$response = $client->interface(
    node   => ['a.example.net', 'b.example.net', 'c.example.net_1'],
    ifName => 'eth0'
);

check_response(21, $response,
    {
      'a.example.net' => {
        '1' => {
          '*node' => 'a.example.net',
          '*intf' => 'eth0',
          'input' => num(410.016806722689, THRESHOLD),
          'output' => num(84.8403361344536, THRESHOLD),
          'inUcast' => num(0.0672268907563025, THRESHOLD),
          'outUcast' => num(0.159663865546218, THRESHOLD),
          'status' => 1,
          'time' => 100124,
        },
      },
      'c.example.net_1' => {
        '1' => {
          '*node' => 'c.example.net',
          '*intf' => 'eth0',
          'input' => num(360.36036036036, THRESHOLD),
          'output' => num(0, THRESHOLD),
          'inUcast' => num(0.225225225225225, THRESHOLD),
          'outUcast' => num(0, THRESHOLD),
          'status' => 1,
          'time' => 100131,
        },
      },
    }
);



# Request 22: reprise of request 16, only with a host which has overlap
# between CPU and interface OID suffixes
$response = $client->test7(
    node => 'd.example.net',
);


check_response(22, $response,
    {
      'd.example.net' => {
        '100' => {
          '*ifName' => undef,
          '*cpuName' => 'CPU1',
          'usageAsFraction' => num(0.14, THRESHOLD),
          'inputOctets' => undef,
          'time' => 100112,
        },
        '101' => {
          '*ifName' => 'eth1',
          '*cpuName' => 'CPU2',
          'usageAsFraction' => num(0.19, THRESHOLD),
          'inputOctets' => num(5.83333333333333, THRESHOLD),
          'time' => any(100112, 100121),
        },
        '104' => {
          '*ifName' => 'eth4',
          '*cpuName' => undef,
          'usageAsFraction' => undef,
          'inputOctets' => num(10.1666666666667, THRESHOLD),
          'time' => 100121,
        },
      },
    }
);



# Request 23: test out some host-variables stuff
$response = $client->test9(
    node => ['a.example.net', 'b.example.net', 'c.example.net_1', 'c.example.net_2', 'd.example.net']
);


# This works unless you're running this test with a system time earlier than when
# I wrote this -- sorry, time travelers.
sub is_timestampy {
   my $num = shift;
   return ($num > 1.5e9);
}

check_response(23, $response,
    {
      'a.example.net' => {
        '1.1' => {
          'ts' => 'hi',
          'cd' => undef,
          'x' => 'a.example.net//hi',
          'time' => any(100135, code(\&is_timestampy)),
        },
        '1.2' => {
          'ts' => 'hi',
          'cd' => undef,
          'x' => 'a.example.net//hi',
          'time' => any(100135, code(\&is_timestampy)),
        },
        '2.3' => {
          'ts' => 'hi',
          'cd' => undef,
          'x' => 'a.example.net//hi',
          'time' => any(100135, code(\&is_timestampy)),
        },
      },
      'b.example.net' => {
        '2' => {
          'ts' => undef,
          'cd' => undef,
          'x' => 'b.example.net//',
          'time' => any(100110, code(\&is_timestampy)),
        },
      },
      'c.example.net_2' => {
        '1' => {
          'ts' => 'lol',
          'cd' => '2',
          'x' => 'c.example.net_2/2/lol',
          'time' => any(100100, code(\&is_timestampy)),
        },
        '2' => {
          'ts' => 'lol',
          'cd' => '2',
          'x' => 'c.example.net_2/2/lol',
          'time' => any(100100, code(\&is_timestampy)),
        },
      },
      'd.example.net' => {
        '100' => {
          'ts' => undef,
          'cd' => '1',
          'x' => 'd.example.net/1/',
          'time' => any(100112, code(\&is_timestampy)),
        },
        '101' => {
          'ts' => undef,
          'cd' => '1',
          'x' => 'd.example.net/1/',
          'time' => any(100112, code(\&is_timestampy)),
        },
      },
    }
);
