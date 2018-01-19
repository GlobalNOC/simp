#!/usr/bin/perl

# This script was used to generate the Redis database used in integration
# tests. Provided you understand the schema Simp uses for Redis, this should
# be... well, *not horrible* to adapt to other test datasets.

use strict;
use warnings;

use Redis;

my @collection_types = ( 'intf', 'cpu' );

my %collection_intervals = (
  'intf' => '60',
  'cpu'  => '300',
);

my %collection_oid_trees = (
  'intf' => [
    '1.3.6.1.2.1.31.1.1.1',
    '1.3.6.1.2.1.2',
  ],
  'cpu'  => [
    '2.10.9994.27.1',
    '2.10.9994.27.8',
  ],
);

my %collection_oid_bases = (
  'intf' => [
    '1.3.6.1.2.1.31.1.1.1.1',  # IF-MIB::ifName
    '1.3.6.1.2.1.31.1.1.1.6',  # IF-MIB::ifHCInOctets
    '1.3.6.1.2.1.31.1.1.1.10', # IF-MIB::ifHCOutOctets
    '1.3.6.1.2.1.2.2.1.11',    # IF-MIB::ifInUcastPkts
    '1.3.6.1.2.1.2.2.1.17',    # IF-MIB::ifOutUcastPkts
    '1.3.6.1.2.1.2.2.1.8',     # IF-MIB::ifOperStatus
  ],
  'cpu'  => [         # Made-up MIB
    '2.10.9994.27.1', # name
    '2.10.9994.27.8', # % utilization
  ],
);

my %workers = (
  'a.example.net'   => { 'intf' => 'intf1', 'cpu' => 'cpu1' },
  'b.example.net'   => { 'intf' => 'intf2', 'cpu' => 'cpu1' },
  'c.example.net_1' => { 'intf' => 'intf2', 'cpu' => 'cpu1' },
  'c.example.net_2' => { 'intf' => 'intf1', 'cpu' => 'cpu1' },
);

my %vars = (
  'a.example.net'   => { 'test' => 'hi' },
  'b.example.net'   => {},
  'c.example.net_1' => { 'test' => 'bye', 'card_no' => '1' },
  'c.example.net_2' => { 'test' => 'lol', 'card_no' => '2' },
);

my %vars_type = (
  'a.example.net'   => 'intf',
  'b.example.net'   => 'cpu',
  'c.example.net_1' => 'intf',
  'c.example.net_2' => 'intf',
);

my %ip_addrs = (
  'a.example.net'   => '10.0.0.1',
  'b.example.net'   => '10.0.0.2',
  'c.example.net_2' => '10.0.0.3',
);

# $collection_data{$type}{$host}[$i] = [$collection_run, $epoch_time (diff from 100_000), $data]
# where $data{$oid_suffix}[$i] = datum corresponding to $i'th entry in $collection_oid_bases{$type}
my %collection_data = (
  'cpu' => {
    'a.example.net' => [
      [ 334, -168, {
                     '1.1' => [ 'CPU1/1', 5 ],
                     '1.2' => [ 'CPU1/2', 1 ],
                     '2.3' => [ 'CPU2/3', 75 ],
                   } ],
      [ 335,  135, {
                     '1.1' => [ 'CPU1/1', 6 ],
                     '1.2' => [ 'CPU1/2', 2 ],
                     '2.3' => [ 'CPU2/3', 71 ],
                   } ],
    ],
    'b.example.net' => [
      [ 1, 110, {
                  '2' => [ 'CPU2', 15 ],
                } ],
    ],
    'c.example.net_2' => [
      [ 24, -201, {
                    '1' => [ 'CPU1', 4 ],
                    '2' => [ 'CPU2', 7 ],
                  } ],
      [ 25,  100, {
                    '1' => [ 'CPU1', 4 ],
                    '2' => [ 'CPU2', 6 ],
                  } ],
    ],
  },
  'intf' => {
    'a.example.net' => [
      [ 1005,   5, {
                     '1' => [ 'eth0', '1000000000001',      '37', '37000', '31000', '1' ],
                     '2' => [ 'eth1',     '300000000', '9000000', '11000', '40000', '1' ],
                   } ],
      [ 1006,  64, {
                     '1' => [ 'eth0', '1000000000100',      '99', '37002', '31001', '1' ],
                     '2' => [ 'eth1',     '300000000', '9000000', '11000', '40000', '1' ],
                   } ],
      [ 1007, 124, {
                     '1' => [ 'eth0', '1000000006100',    '1299', '37008', '31019', '1' ],
                     '2' => [ 'eth1',     '300003000', '9000120', '11006', '40240', '0' ],
                   } ],
    ],
    'b.example.net' => [
      [ 372,  12, {
                    '1' => [ 'eth1', '100050', '200001', '1000', '1100', '1' ],
                    '3' => [ 'eth2', '300060',      '2', '1000', '1100', '1' ],
                  } ],
      [ 373,  72, {
                    '1' => [ 'eth1', '100150', '200011', '1003', '1101', '1' ],
                    '3' => [ 'eth2', '300080',     '32', '1001', '1101', '0' ],
                  } ],
      [ 374, 132, {
                    '1' => [ 'eth1', '100750', '200041', '1009', '1161', '1' ],
                    '3' => [ 'eth2', '301280',     '92', '1121', '1107', '1' ],
                  } ],
    ],
    'c.example.net_1' => [
      [ 801,  20, {
                     '1' => [ 'eth0', '10000', '1000', '100', '200', '1' ],
                  } ],
      [ 802,  81, {
                     '1' => [ 'eth0', '10000', '1000', '100', '200', '1' ],
                  } ],
      [ 803, 131, {
                     '1' => [ 'eth0', '15000', '1000', '125', '200', '1' ],
                  } ],
    ],
    'c.example.net_2' => [
      [ 803, 135, {
                     '8' => [ 'eth7', '8898987987', '6758799', '354', '75476', '1' ],
                  } ],
    ],
  },
);

my $r = Redis->new(
    server    => '127.0.0.1:6380',
    reconnect => 60,
    every     => 1_000_000,
);

foreach my $type (keys %collection_data) {
  foreach my $host (keys %{$collection_data{$type}}) {
    my $measurements = $collection_data{$type}{$host};

    foreach my $run (@$measurements) {

      my $time = $run->[1] + 100_000;
      my $key0 = $host . ',' . $workers{$host}{$type} . ',' . $time;

      $r->select(0);

      my $oid_bases = $collection_oid_bases{$type};

      foreach my $oid_suffix (keys %{$run->[2]}) {
        for (my $i = 0; $i < scalar(@$oid_bases); $i++) {
          $r->sadd($key0, $oid_bases->[$i] . '.' . $oid_suffix . ',' . $run->[2]{$oid_suffix}[$i]);
        }
      }

      if ($type eq $vars_type{$host}) {
        foreach my $name (keys %{$vars{$host}}) {
          $r->sadd($key0, 'vars.' . $name . ',' . $vars{$host}{$name});
        }
      }

      $r->select(1);

      $r->set($host . ',' . $type . ',' . $run->[0], $key0);
      $r->set($ip_addrs{$host} . ',' . $type . ',' . $run->[0], $key0) if defined($ip_addrs{$host});

      $r->select(2);

      $r->set($host . ',' . $type, $run->[0] . ',' . $time);
      $r->set($ip_addrs{$host} . ',' . $type, $run->[0] . ',' . $time) if defined($ip_addrs{$host});
    }
  }
}

# Handle DB 3
$r->select(3);

foreach my $host (keys %workers) {
  foreach my $type (@collection_types) {
    my $val = $type . ',' . $collection_intervals{$type};
    foreach my $oid (@{$collection_oid_trees{$type}}) {
      $r->hset($host, $oid, $val);
      $r->hset($ip_addrs{$host}, $oid, $val) if defined($ip_addrs{$host});
    }

    if ($type eq $vars_type{$host}) {
      $r->hset($host, 'vars', $val);
      $r->hset($ip_addrs{$host}, 'vars', $val) if defined($ip_addrs{$host});
    }
  }
}

