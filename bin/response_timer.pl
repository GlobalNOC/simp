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

    $config_file = "/etc/simp/response-graph-config.xml";
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
    # $after = 5;   

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

            # my ($requests,$average_time) = _get_requests();
            my ($simp_data, $comp_data) = _get_requests();

            # my $ping_latency = _get_ping_latency();
            my $ping_latency = 0; # Uncomment above

            # my $res = _push_results( requests => $requests,
            #     average_time => $average_time,
            #     ping => $ping_latency,
            #     app => $app);
            
            my $res = _push_results($simp_data, $comp_data, $ping_latency);

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

    my $simp_time = 0;
    my $simp_count = 0;
    my $simp_avg = 0; 
    
    my $comp_time = 0;
    my $comp_count = 0;
    my $comp_avg = 0; 
    
    my (@lines) = nonblockGetLines($fh);
    foreach my $line ( @lines ) {
        if ($line eq 0) {
            next;
        } 
        print "Line: $line \n"; 
        my @split_str = split / /, $line;

        if ($split_str[-2] eq "SIMP") {
            $simp_count++; 
            $simp_time += $split_str[-1];
         
        } else {
            $comp_count++;
            $comp_time = $split_str[-1];
        } 
    }

    if ($simp_count != 0) {		
        $simp_avg = $simp_time/$simp_count;
    } else {
        $simp_avg = 0;
    }


    if ($comp_count != 0) {		
        $comp_avg = $comp_time/$comp_count;
    } else {
        $comp_avg = 0;
    }

    print "\n";
    print "After $after seconds: \n";
    print "Simp total time: $simp_time \n";
    print "Simp count: $simp_count \n";
    print "Simp average: $simp_count \n";
    print "Comp total time: $comp_time \n";
    print "Comp count: $comp_count \n";
    print "Comp average $comp_avg \n";
    


    return ({simp_avg => $simp_avg, simp_count => $simp_count}, {comp_avg => $comp_avg, comp_count => $comp_count});
}


#--- Push data into TSDS
sub _push_results {
    # my @push; 
    # my %params = @_;
    # my $requests = $params{'requests'};
    # my $average_time = $params{'average_time'};
    # my $ping_latency = $params{'ping'};
    # my $app = $params{'app'};
    # print "Request: $requests \n";
    # print "Average: $average_time \n";
    # print "Ping: $ping_latency \n"; 

    # my %meta_data;
    # $meta_data{'node'} = "simp.bldc.grnoc.iu.edu";
    # $meta_data{'app'} = "simp-data";

    # my %values;
    # $values{'requests'} = $requests;
    # $values{'average_response'} = $average_time; 
    # $values{'ping'} = $ping_latency;

    # my %push_data;
    # $push_data{'interval'} = 60;
    # $push_data{'time'} = time() ;
    # $push_data{'type'} = "simp_requests" ;
    # $push_data{'meta'} = \%meta_data;
    # $push_data{'values'} = \%values ;

    # push @push, \%push_data;
    # my $data    = encode_json \@push;
    # 
    # print Dumper($data);
    # # Push data
    # # my $result = $svc->add_data(data => $data);
    # # print Dumper($result);

    # print "--------------------\n";
    
    
    
    
    # my @push; 
    # my %params = @_;
    my $simp_data = shift;
    my $comp_data = shift;
    my $ping_latency = shift;
    print Dumper($simp_data);
    print Dumper($comp_data);
    print Dumper($ping_latency);
    # my $requests = $params{'requests'};
    # my $average_time = $params{'average_time'};
    # my $ping_latency = $params{'ping'};
    # my $app = $params{'app'};
    # print "Request: $requests \n";
    # print "Average: $average_time \n";
    # print "Ping: $ping_latency \n"; 

    # For simp 
    my %simp_meta_data;
    $simp_meta_data{'node'} = "simp.bldc.grnoc.iu.edu";
    $simp_meta_data{'app'} = "simp-data";

    my %simp_values;
    $simp_values{'requests'} = $simp_data->{'simp_count'};
    $simp_values{'average_response'} = $simp_data->{'simp_avg'};
    $simp_values{'ping'} = $ping_latency;

    my %simp_push_data;
    $simp_push_data{'interval'} = 60;
    $simp_push_data{'time'} = time() ;
    $simp_push_data{'type'} = "simp_requests" ;
    $simp_push_data{'meta'} = \%simp_meta_data;
    $simp_push_data{'values'} = \%simp_values ;

    # push @push, \%push_data;
     
    # print Dumper(@push); 
    
    # undef %meta_data; 
    # undef %values;
    # undef %push_data;
    
    # For comp 
    my %comp_meta_data; 
    $comp_meta_data{'node'} = "simp.bldc.grnoc.iu.edu";
    $comp_meta_data{'app'} = "simp-comp";

    my %comp_values;
    $comp_values{'requests'} = $comp_data->{'comp_count'};
    $comp_values{'average_response'} = $comp_data->{'comp_avg'};
    $comp_values{'ping'} = $ping_latency;

    my %comp_push_data;
    $comp_push_data{'interval'} = 60;
    $comp_push_data{'time'} = time() ;
    $comp_push_data{'type'} = "simp_requests" ;
    $comp_push_data{'meta'} = \%comp_meta_data;
    $comp_push_data{'values'} = \%comp_values ;

    my @push =  (\%simp_push_data, \%comp_push_data);
    
    print Dumper(@push); 
    my $data    = encode_json \@push;
    
    print Dumper($data);
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
