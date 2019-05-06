package GRNOC::Simp::TSDS::Worker;

use strict;
use warnings;

use Moo;
use AnyEvent;
use Data::Dumper;

use List::MoreUtils qw(any);

use GRNOC::RabbitMQ::Client;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Method;
use GRNOC::Simp::TSDS;
use GRNOC::Simp::TSDS::Pusher;

=head2 public attributes

=over 12

=item logger

=item worker_name

=item simp_config

=item tsds_config

=item tsds_type

=item hosts

=item interval

=item composite_name

=item filter_name

=item filter_value

=item exclude_patterns

=item required_value_fields

=back

=cut

has worker_name => (is => 'ro',
		    required => 1);

has logger => (is => 'rwp',
	       required => 1);

has simp_config => (is => 'rwp',
		    required => 1);

has tsds_config => (is => 'rwp',
		    required => 1);

has hosts => (is => 'rwp',
	      required => 1);

has tsds_type => (is => 'rwp',
                  required => 1);

has interval => (is => 'rwp',
		 required => 1);

has composite_name => (is => 'rwp',
		       required => 1);

has filter_name => (is => 'rwp');
has filter_value => (is => 'rwp');

has exclude_patterns => (is => 'rwp', default => sub { [] });

has required_value_fields => (is => 'rwp', default => sub { [] });


=head2 private attributes

=over 12

=item simp_client

=item tsds_pusher

=item poll_w

=item push_w

=item msg_list

=item cv

=item stop_me

=back 

=cut

has simp_client => (is => 'rwp');
has tsds_pusher => (is => 'rwp');
has poll_w => (is => 'rwp');
has push_w => (is => 'rwp');
has msg_list => (is => 'rwp', default => sub { [] });
has cv => (is => 'rwp');
has stop_me => (is => 'rwp', default => 0);

=head2 run

=cut
sub run {
    my ($self) = @_;

    # change process name
    $0 = "simp_tsds(" . $self->worker_name . ")";

    # Set logging object
    $self->_set_logger(Log::Log4perl->get_logger('GRNOC.Simp.TSDS.Worker'));

    # Set worker properties
    $self->_load_config();

    # Enter event loop, loop until condvar met
    $self->logger->info("Entering event loop");
    $self->_set_cv(AnyEvent->condvar());
    $self->cv->recv;

    $self->logger->info($self->worker_name . " loop ended, terminating");
}

#
# Load config
#
sub _load_config {
    my ($self) = @_;
    $self->logger->info($self->worker_name . " starting");

    # Create dispatcher to watch for messages from Master
    my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new(
	host => $self->simp_config->{'host'},
	port => $self->simp_config->{'port'},
	user => $self->simp_config->{'user'},
	pass => $self->simp_config->{'password'},
	exchange => 'SNAPP',
	topic    => "SNAPP." . $self->worker_name
    );

    # Create and register stop method
    my $stop_method = GRNOC::RabbitMQ::Method->new(
	name        => "stop",
	description => "stops worker",
	callback => sub {
	    $self->_set_stop_me(1);
	}
    );
    $dispatcher->register_method($stop_method);

    # Create SIMP client object
    $self->_set_simp_client(GRNOC::RabbitMQ::Client->new(
	host => $self->simp_config->{'host'},
	port => $self->simp_config->{'port'},
	user => $self->simp_config->{'user'},
	pass => $self->simp_config->{'password'},
	exchange => 'Simp',
	topic    => 'Simp.CompData'
    ));

    # Create TSDS Pusher object
    $self->_set_tsds_pusher(GRNOC::Simp::TSDS::Pusher->new(
	logger => $self->logger,
	worker_name => $self->worker_name,
	tsds_config => $self->tsds_config,
    ));
    
    # set interval
    my $interval = $self->interval;

    # set composite name
    my $composite = $self->composite_name;

    # Create polling timer for event loop
    $self->_set_poll_w(AnyEvent->timer(
	after => 5,
	interval => $interval,
	cb => sub {
	    my $tm = time;
	
	    # Pull data for each host from Comp
	    foreach my $host (@{$self->hosts}) {
		$self->logger->info($self->worker_name . " processing $host");
		
		my %args = (
		    node           => $host,
		    period         => $interval,
		    async_callback => sub {
			my $res = shift;
			
			# Process results and push when idle
			$self->_process_host($res, $tm);
			$self->_set_push_w(AnyEvent->idle(cb => sub { $self->_push_data; }));
		    }
		    );

		# if we're trying to only get a subset of values out of simp,
 		# add those arguments now. This presumes that SIMP and SIMP-collector
		# have been correctly configured to have the data available
		# validity is checked for earlier in Master
		if ($self->filter_name){
		    $args{$self->filter_name} = $self->filter_value;
		}

                # to provide some degree of backward compatibility, we only put this field on if we need to:
                if (scalar(@{$self->exclude_patterns}) > 0) {
                    $args{'exclude_regexp'} = $self->exclude_patterns;
                }

		$self->simp_client->$composite(%args);
	    }

	    # Push when idle
	    $self->_set_push_w(AnyEvent->idle(cb => sub { $self->_push_data; }));
	}
    ));
    
    $self->logger->info($self->worker_name . " Done setting up event callbacks");
}

#
# Process host for publishing to TSDS
#
sub _process_host {
    my ($self, $res, $tm) = @_;

    # Drop out if we get an error from Comp
    if (!defined($res) || $res->{'error'}) {
	$self->logger->error($self->worker_name . " Comp error: " . GRNOC::Simp::TSDS::error_message($res));
	return;
    }

    my $required_values = $self->required_value_fields;

    # Take data from Comp and "package" for a post to TSDS
    foreach my $node_name (keys %{$res->{'results'}}) {

	$self->logger->debug($self->worker_name . ' Name: ' . Dumper($node_name));
	$self->logger->debug($self->worker_name . ' Value: ' . Dumper($res->{'results'}->{$node_name}));

	my $data = $res->{'results'}->{$node_name};
	foreach my $datum (@$data) {
	    my %vals;
	    my %meta;
	    my $datum_tm = $tm;

            # this works properly when required_value_fields is empty,
            # as any returns false then
            next if (any { !defined($datum->{$_}) } @$required_values);

	    foreach my $key (keys %{$datum}) {
                # This is commented out for now due to a bug in TSDS (3135:160)
                # where bad things happen if a key is sent some of the time
                # (as opposed to all the time or none of the time):
                #next if !defined($datum->{$key});

		if ($key eq 'time') {
		    next if !defined($datum->{$key}); # workaround for 3135:160
		    $datum_tm = $datum->{$key} + 0;
		} elsif ($key =~ /^\*/) {
		    my $meta_key = substr($key, 1);
		    $meta{$meta_key} = $datum->{$key};
		} else {
                    # this can be simplified once 3135:160 is fixed
		    $vals{$key} = (defined($datum->{$key})) ? $datum->{$key} + 0 : undef;
		}
	    }

	    # push onto our queue for posting to TSDS
	    push @{$self->msg_list}, {
		type => $self->tsds_type,
		time => $datum_tm,
		interval => $self->interval,
		values => \%vals,
		meta => \%meta
	    };
	}
    }
}

#
# Push to TSDS
#
sub _push_data {
    my ($self) = @_;
    my $msg_list = $self->msg_list;
    my $res = $self->tsds_pusher->push($msg_list);
    $self->logger->debug( Dumper($msg_list) );
    $self->logger->debug( Dumper($res) );

    unless ($res) {
	# If queue is empty and stop flag is set, end event loop
	$self->cv->send() if $self->stop_me;
	# Otherwise clear push timer
	$self->_set_push_w(undef);
	exit(0) if $self->stop_me;
    }
}

1;
