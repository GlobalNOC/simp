# A bunch of helper functions for simp testing to avoid having to
# redefine over and over

package SimpTesting;

use strict;
use warnings;
use Data::Dumper;
use JSON::XS;
use FindBin;

use GRNOC::Simp::Comp;
use GRNOC::Simp::Comp::Worker;

use Moo;

has data_set_name => (
    is       => 'ro',
    required => 1
    );

has data_set => (
    is       => 'rwp'
);

has mocked_comp => (
    is => 'rwp'
);

has comp => (
    is       => 'rw'    
);

sub BUILD {
    my ($self) = @_;

    $self->_load_data_set();

    $self->_mock_comp();

    return $self;
}


# Helper function used to load one of the testing datasets into JSON
# object. Since the pieces of simp communicate via JSON this is used to
# simulate the response from one piece to another
sub _load_data_set {
    my $self = shift;

    my $name = $self->data_set_name;

    my $full_path = $FindBin::Bin . "/conf/data_sets/$name" . ".json";

    open(my $fh, "<", $full_path) or die "Can't open $full_path: $!";
    my $lines = join("\n",<$fh>);
    close($fh);

    my $json = JSON::XS::decode_json($lines);

    $self->_clean_comments($json);
    $self->_set_data_set($json);
}

# We allow the key '###' as "comments" in the testing datasets, this will strip it out
# so that the actual tests won't see it. This is kind of a hack but no comments in 
# JSON is =/
sub _clean_comments {
    my $self = shift;
    my $hash = shift;
    return if (! ref $hash);

    foreach my $key (keys %$hash){
	if ($key =~ /^#/){
	    delete $hash->{$key};
	    next;
	}
	$self->_clean_comments($hash->{$key});
    }
}

# Helper function used in Mock for Simp/Comp to simulate talking to Simp/Data.
# This will look up the response it would receive from the test dataset specified
sub _mock_comp_handler {
    my $testing_self = shift;

    return sub {
	my ($comp_self, %args) = @_;
	
	# These are the only args passed to RMQ client in Comp, so we can
	# intercept and do whatever we need to here.
	my $node     = $args{'node'}->[0];
	my $period   = $args{'period'};
	my $oidmatch = $args{'oidmatch'};
	my $callback = $args{'async_callback'};
	
	# Sometimes this is passed as an array, sometimes string.
	$oidmatch = $oidmatch->[0] if(ref($oidmatch) eq 'ARRAY');

	warn "Getting data for $node -> $oidmatch";

	my $res = $testing_self->data_set->{$node}{$oidmatch};

	my $data = {"results" => {"$node" => $res}};
	
	&$callback($data);
    };
}

# This will return an instance of Simp::Comp::Worker that has its methods for
# retrieving data from Simp::Data mocked away so that we can more easily
# test specific input/outputs
sub _mock_comp {
    my $self = shift;

   my $request_handler = $self->_mock_comp_handler();

    # mock the RMQ client so we can fake making requests to Data. We only care about
    # `get` and `get_rate` in comp so we have a faux class for that
    my $mock_client  = Test::MockObject->new();
    $mock_client->mock('get',  sub { &$request_handler(@_); });
    $mock_client->mock('get_rate',  sub { &$request_handler(@_); });
   
    my $mock_comp_worker = Test::MockModule->new('GRNOC::Simp::Comp::Worker');
    # we're not going to use RMQ for anything here, so mock away any actual connections
    # while we're creating this object
    $mock_comp_worker->mock('_setup_rabbitmq', sub { return 1; } ); 
    $mock_comp_worker->mock('client', sub { return $mock_client; } ); 

    # We need to keep this in scope or else the mock stops working
    $self->_set_mocked_comp($mock_comp_worker);

    # We need to make the Comp Master first to do all the composite generation and validation
    my $mock_comp = Test::MockModule->new('GRNOC::Simp::Comp');
    $mock_comp->mock('_validate_config', sub { 1; }); #TODO: actually do this, skipping validation is bad mmkay

    # TODO - fix up all the junk values
    my $comp_master = GRNOC::Simp::Comp->new(composite_xsd => 'asdf',
					     composites_dir => $FindBin::Bin . "/conf/composites/",
					     config_file => 'asdf', 
					     config_xsd => 'adf',
					     logging_file => $FindBin::Bin . "/conf/compDataLogging.conf");
    my $composites = $comp_master->composites();
    
    my $comp_worker = GRNOC::Simp::Comp::Worker->new(config     => $comp_master->config,
						     logger     => $comp_master->logger,
						     composites => $composites,
						     daemonize  => 0,
						     worker_id  => 'testing');

    $self->comp($comp_worker);
}

sub comp_get {
    my $self      = shift;
    my $node      = shift;
    my $comp_name = shift;

    my $composite = $self->comp->composites->{$comp_name};

    my $final_data;

    $self->comp->_get($composite, {"success_callback" => sub { $final_data = shift; }}, {node => {'value' => [$node]}});

    return $final_data;
}

1;
