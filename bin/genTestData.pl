#!/usr/bin/perl 
#--- script that will put a bunch of fake data into redis so we can create a bit of work for redis
use strict;
use Data::Dumper;
use Redis;


sub genHash{
  my $redis  = shift;

  #1.3.6.1.2.1.2.2.1.
  my $oid_str   = "1.3.6.1.2.1.2.2.1.";
  my $host_str  = "10.13.13.";

  for(my $octet_d = 0;$octet_d<=255;$octet_d++){

    my $host = $host_str.$octet_d;
    print "inserting data for host: $host\n";
    for(my $x=0;$x<50;$x++){
      for(my $y=0;$y<50;$y++){
	my $oid  = $oid_str.$x.".".$y;
	my $val  = $y;
	#print "$oid -> $host : $val\n";
         $redis->hset($oid,$host,$val,sub {});
      }
    }
    my $ts = gmtime();
    $redis->hset("ts",$host,$ts);
    #$redis->wait_all_responses; 
  }
}



 my$redis = Redis->new(server => '127.0.0.1:6379');
 genHash($redis);
