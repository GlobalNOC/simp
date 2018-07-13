#/usr/bin/perl
use Time::HiRes qw(usleep gettimeofday tv_interval);
use strict;
use warnings;
use Data::Dumper;
use GRNOC::RabbitMQ::Client;
use AnyEvent;
use JSON;
use GRNOC::WebService::Client;
use IO::File;


my $fifo_file;
my $fh;
my $after;
my $client;
my $svc;
my $config_file;
my $tsds_config;
my $simp_config;
#--- Main method
sub main{
    
    $config_file = "response-graph-config.xml";
    my $config = GRNOC::Config->new(config_file => $config_file, force_array => 0, debug => 0); 
    

    $fifo_file = $config->get("/config/pipe")->{'fifofile'};
    $fh = IO::File->new($fifo_file, 'r');
    
    $tsds_config = $config->get("/config/tsds");
    $svc = GRNOC::WebService::Client->new(
        url    => $tsds_config->{'url'},
        uid    => $tsds_config->{'user'},
        passwd    => $tsds_config->{'password'},
        usePost    => 1,
        debug => 0
    );
    $after = $tsds_config->{'refresh'};   
    
    $simp_config = $config->get("/config/simp");
    $client = GRNOC::RabbitMQ::Client->new(   host => $simp_config->{'host'},
        port => $simp_config->{'port'},
        user => $simp_config->{'user'},
        pass => $simp_config->{'password'},
        exchange => 'Simp',
        timeout => 60,
        topic => 'Simp.Data');

    
    my $cv = AnyEvent->condvar;

    my $once_per_second = AnyEvent->timer (
        after => $after,
        interval => $after,
        cb => sub {

            my ($requests,$average_time) = _get_requests();

            # my $ping_latency = _get_ping_latency();
            my $ping_latency = 0; # Uncomment above

            my $res = _push_results( requests => $requests,
                average_time => $average_time,
                ping => $ping_latency );

        }
    );

    $cv->recv;

    close $fh;
    exit(0);
}

#--- An non-blocking filehandle read that returns an array of lines read
my %nonblockGetLines_last;
sub nonblockGetLines {
    my ($fh,$timeout) = @_;
    $timeout = 0 unless defined $timeout;
    my $rfd = '';
    $nonblockGetLines_last{$fh} = ''
    unless defined $nonblockGetLines_last{$fh};

    vec($rfd,fileno($fh),1) = 1;
    return unless select($rfd, undef, undef, $timeout)>=0;
    # I'm not sure the following is necessary?
    return unless vec($rfd,fileno($fh),1);
    my $buf = '';
    my $n = sysread($fh,$buf,1024*1024);
    # If we're done, make sure to send the last unfinished line
    return ($nonblockGetLines_last{$fh}) unless $n;
    # Prepend the last unfinished line
    $buf = $nonblockGetLines_last{$fh}.$buf;
    # And save any newly unfinished lines
    $nonblockGetLines_last{$fh} =
    (substr($buf,-1) !~ /[\r\n]/ && $buf =~ s/([^\r\n]*)$//) ? $1 : '';
    $buf ? (0,split(/\n/,$buf)) : (0);
}


#--- Get number of requests per minute and average response time 
sub _get_requests() {

    my $count = 0;
    my $time_sum = 0;
    
    my $avg_time; 
    my (@lines) = nonblockGetLines($fh);
    foreach my $line ( @lines ) {
        if ($line eq 0) {
            next;
        } 
        $count++;
        print "Line: $line \n"; 
        my $res_time = (split / /, $line)[-1];
        # print "Printing time: $res_time\n";
        $time_sum += $res_time;
    }


    print "\n";
    print "After $after seconds: \n";
    # print "Request count: $count\n";
    print "Time sum: $time_sum\n";

    if ($count != 0) {		
        $avg_time = $time_sum/$count;
        # print "Average time: $avg_time\n";
    } else {
        $avg_time = 0;
        # print "Average time: 0\n";
    }

    return ($count, $avg_time);
}


#--- Push data into TSDS
sub _push_results {
    my @push; 
    my %params = @_;
    my $requests = $params{'requests'};
    my $average_time = $params{'average_time'};
    my $ping_latency = $params{'ping'};
    print "Request: $requests \n";
    print "Average: $average_time \n";
    print "Ping: $ping_latency \n"; 

    my %meta_data;
    $meta_data{'node'} = "simp.bldc.grnoc.iu.edu";
    $meta_data{'app'} = "simp-data";

    my %values;
    $values{'requests'} = $requests;
    $values{'average_response'} = $average_time; 
    $values{'ping'} = $ping_latency;

    my %push_data;
    $push_data{'interval'} = 60;
    $push_data{'time'} = time() ;
    $push_data{'type'} = "simp_requests" ;
    $push_data{'meta'} = \%meta_data;
    $push_data{'values'} = \%values ;

    push @push, \%push_data;
    my $data    = encode_json \@push;

    # Push data
    my $result = $svc->add_data(data => $data);
    # print Dumper($result);

    print "--------------------\n";
}
#--- Get the ping time
sub _get_ping_latency() {
    my $results;
    my $start = [gettimeofday];
    $results = $client->ping();
    my $end = [gettimeofday];
    my $time = tv_interval($start, $end);
    print "Time: $time";
    return $time;

}

#---
main();
