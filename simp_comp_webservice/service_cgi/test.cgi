#!/usr/bin/perl -w
print "Content-type: text/html\r\n\r\n";
use LWP::Simple;
require HTTP::Request;
$request = HTTP::Request->new(GET =>'http://pmorpari-dev.grnoc.iu.edu/cgi-bin/web-services-example.cgi?method=count_objects2');
$ua = LWP::UserAgent->new;
$response = $ua->request($request);
print $response->status_line();
print "<br>";
print $response->content();
print "<br>";
die "Couldn't get it!" unless defined $response;
if ($response->is_success) {

    print $response->decoded_content;  # or whatever
}
else {
    die $response->status_line;
}
print "Hello there!<br />\nJust testing .<br />\n";
print scalar keys %response;
print $response;
#$contents = get("http://pmorpari-dev.grnoc.iu.edu/cgi-bin/web-services-example.cgi?method=count_objects");
#die "Couldn't get it!" unless defined $content;
foreach (sort keys %response) {
    	print "a";
	print "<br>";
	print "$_ : $response{$_}\n";
 }
my @keys	= keys %response;

while (@keys){
	print pop(@keys);
}
for ($i=0; $i<10; $i++)
{


print $i."<br />";


}
