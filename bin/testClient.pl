#!/usr/bin/perl
#--- test client to help understand the functionality and performance of the Simp.Data rabbit service
use Time::HiRes qw(usleep gettimeofday tv_interval);
use strict;
use Data::Dumper;
use GRNOC::RabbitMQ::Client;
use AnyEvent;

my $client = GRNOC::RabbitMQ::Client->new(   host => "127.0.0.1",
					     port => 5672,
					     user => "guest", 
					     pass => "guest",
					     exchange => 'Simp',
					     timeout => 1,
					     topic => 'Simp.Data');
my $results;
my $start;
my $sum = 0;
my $n = 200;
for(my $x=0;$x<$n;$x++){
  $start = gettimeofday();
  $results = $client->get(
    ipaddrs   => ["10.13.13.100","10.13.13.101","10.13.13.102","10.13.13.103","10.13.13.104","10.13.13.105","10.13.13.106","10.13.13.107","10.13.13.108","10.13.13.109","10.13.13.110","10.13.13.111","10.13.13.112","10.13.13.113","10.13.13.114","10.13.13.115","10.13.13.116","10.13.13.117","10.13.13.118","10.13.13.119","10.13.13.120","10.13.13.121","10.13.13.122","10.13.13.123"],
    oidmatch  => "1.3.6.1.2.1.2.2.1.1[16].*",
    #oidmatch  => "1.3.6.1.2.1.2.2.1.11.*"
  );
  my $et = gettimeofday() - $start;
  $sum += $et;
  if($x % 50 == 0){
    print "et = $et\n";
    print Dumper($results);
  };
}

my $per = ($sum * 1.0) / ($n *1.0);
print "et avg = $per sum=$sum  n=$n\n";



