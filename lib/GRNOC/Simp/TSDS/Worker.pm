package GRNOC::Simp::TSDS::Worker;

use strict;
use warnings;

use AnyEvent;
use Data::Dumper;
use List::MoreUtils qw(any);
use Moo;

use GRNOC::RabbitMQ::Client;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Method;
use GRNOC::Simp::TSDS;
use GRNOC::Simp::TSDS::Pusher;

=head2 public attributes
=over 12

=item logger
=item worker_name
=item rabbitmq
=item tsds_config
=item measurement_type
=item hosts
=item interval
=item composite
=item filter_name
=item filter_value
=item exclude_patterns
=item required_values

=back
=cut

has worker_name => (
    is       => 'ro',
    required => 1
);

has logger => (
    is       => 'rwp',
    required => 1
);

has rabbitmq => (
    is       => 'rwp',
    required => 1
);

has tsds_config => (
    is       => 'rwp',
    required => 1
);

has hosts => (
    is       => 'rwp',
    required => 1
);

has measurement_type => (
    is       => 'rwp',
    required => 1
);

has interval => (
    is       => 'rwp',
    required => 1
);

has composite => (
    is       => 'rwp',
    required => 1
);

has filter_name  => (is => 'rwp');
has filter_value => (is => 'rwp');

has exclude_patterns => (
    is      => 'rwp',
    default => sub { [] }
);

has required_values => (
    is      => 'rwp',
    default => sub { [] }
);

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
has poll_w      => (is => 'rwp');
has push_w      => (is => 'rwp');
has cv          => (is => 'rwp');

has msg_list => (
    is      => 'rwp',
    default => sub { [] }
);

has stop_me => (
    is      => 'rwp',
    default => 0
);

=head2 run

=cut
sub run {
    my ($self) = @_;

    # change process name
    $0 = "simp_tsds(" . $self->worker_name . ")";

    # Set logging object
    $self->_set_logger(Log::Log4perl->get_logger('GRNOC.Simp.TSDS.Worker'));
    $self->logger->debug('SIMP-TSDS WORKER');

    # Connect and set the RabbitMQ Dispatcher and Client
    # The worker will be locked on this step until Rabbitmq has connected
    $self->_setup_rabbitmq();

    # Set worker properties
    $self->_load_config();

    # Enter event loop, loop until condvar met
    $self->logger->info("Entering event loop");
    $self->_set_cv(AnyEvent->condvar());
    $self->cv->recv;

    $self->logger->info($self->worker_name . " loop ended, terminating");
}


=head2 _setup_rabbitmq()
    This will connect the worker to RabbitMQ.
    If the worker fails to connect on the first try, it will continue to retry until connected.
=cut
sub _setup_rabbitmq {

    my $self = shift;

    # We use this flag to indicate whether RMQ has connected.
    # If it hasn't, we continue to retry connection.
    my $connected = 0;

    while (!$connected) {

        my $client = GRNOC::RabbitMQ::Client->new(
            host     => $self->rabbitmq->{'ip'},
            port     => $self->rabbitmq->{'port'},
            user     => $self->rabbitmq->{'user'},
            pass     => $self->rabbitmq->{'password'},
            exchange => 'Simp',
            topic    => 'Simp.Comp'
        );

        my $dispatcher = GRNOC::RabbitMQ::Dispatcher->new(
            host     => $self->rabbitmq->{'ip'},
            port     => $self->rabbitmq->{'port'},
            user     => $self->rabbitmq->{'user'},
            pass     => $self->rabbitmq->{'password'},
            exchange => 'SNAPP',
            topic    => "SNAPP.$self->worker_name"
        );

        # Check whether the Client and Dispatcher are connected
        # 0 = One or Both are not connected
        # 1 = Connected Both
        $connected = ($client && $client->connected) && ($dispatcher && $dispatcher->connected) ? 1 : 0;

        # Log an error and wait to retry if we couldn't connect
        unless ($connected) {
            $self->logger->error('GRNOC.Simp.TSDS.Worker could not connect to RabbitMQ, retrying...');
            sleep 2;
        }
        # Finish setup once the Client and Dispatcher are connected
        else {

            # Set the client
            $self->_set_simp_client($client);

            # Create and register the Dispatcher's Stop method
            my $stop = GRNOC::RabbitMQ::Method->new(
                name        => 'stop',
                description => 'Stops the TSDS Worker',
                callback    => sub { $self->_set_stop_me(1); }
            );
            $dispatcher->register_method($stop);

            # Log a success message
            $self->logger->debug('RabbitMQ Client and Dispatcher have connected!')
        }
    }
}


sub _load_config {

    my ($self) = @_;

    $self->logger->info($self->worker_name . " starting");

    # Get the interval and composite name
    my $interval  = $self->interval;
    my $composite = $self->composite;

    # Create and set the TSDS Pusher object
    my $pusher = GRNOC::Simp::TSDS::Pusher->new(
        logger      => $self->logger,
        worker_name => $self->worker_name,
        tsds_config => $self->tsds_config,
    );
    $self->_set_tsds_pusher($pusher);

    # Create polling timer for event loop
    $self->_set_poll_w(
        AnyEvent->timer(
            after    => 5,
            interval => $interval,
            cb       => sub {
                my $tm = time;

                # Pull data for each host from Comp
                for my $host (@{$self->hosts}) {

                    $self->logger->info($self->worker_name . " processing $host");

                    my %args = (
                        node           => $host,
                        period         => $interval,
                        async_callback => sub {

                            my $res = shift;

                            # Process results and push when idle
                            $self->_process_data($res, $tm);
                            $self->_set_push_w(AnyEvent->idle(cb => sub { $self->_push_data; }));
                        }
                    );

                    # if we're trying to only get a subset of values out of
                    # simp, add those arguments now. This presumes that SIMP
                    # and SIMP-collector have been correctly configured to have
                    # the data available validity is checked for earlier
                    # in Master
                    if ($self->filter_name) {
                        $args{$self->filter_name} = $self->filter_value;
                    }

                    # to provide some degree of backward compatibility,
                    # we only put this field on if we need to:
                    if (scalar(@{$self->exclude_patterns}) > 0) {
                        $args{'exclude_regexp'} = $self->exclude_patterns;
                    }

                    $self->simp_client->$composite(%args);
                }

                # Push when idle
                $self->_set_push_w(AnyEvent->idle(cb => sub { $self->_push_data; }));
            }
        )
    );

    $self->logger->info($self->worker_name . " Done setting up event callbacks");
}


=head2 _process_data
    This will process data from Comp into TSDS-friendly messages.
    All data is processed before it can be posted to TSDS.
    Metadata and Value fields are separated here.
=cut
sub _process_data {
    my ($self, $res, $tm) = @_;

    # Drop out if we get an error from Comp
    if (!defined($res) || $res->{'error'}) {
        $self->logger->error($self->worker_name." Comp error: ".GRNOC::Simp::TSDS::error_message($res));
        return;
    }

    # Take data from Comp by node and process for posting to TSDS
    for my $node_name (keys %{$res->{'results'}}) {

        #$self->logger->debug($self->worker_name . ' Name: ' . Dumper($node_name));
        #$self->logger->debug($self->worker_name.' Value: '. Dumper($res->{'results'}->{$node_name}));

        my $data = $res->{'results'}->{$node_name};

        # Process every data object/hash for the node
        for my $datum (@$data) {

            my %vals;
            my %meta;
            my $datum_tm = $tm;

            # When required_values are empty, skip the data
            next if (any { !defined($datum->{$_}) && !defined($datum->{"*$_"}) } @{$self->required_values});

            # Check the keys in the data and separate metadata and value fields
            for my $key (keys %{$datum}) {

                # This is commented out for now due to a bug in TSDS (3135:160)
                # where bad things happen if a key is sent some of the time
                # (as opposed to all the time or none of the time):
                # next if !defined($datum->{$key});
                if ($key eq 'time') {
                    next if !defined($datum->{$key});  # workaround for 3135:160
                    $datum_tm = $datum->{$key} + 0;
                }
                # Keys for metadata start with an asterisk (*)
                elsif ($key =~ /^\*/) {
                    $meta{substr($key, 1)} = $datum->{$key};
                }
                # Keys for values do not have an asterisk
                else {
                    $vals{$key} = defined($datum->{$key}) ? $datum->{$key} + 0 : undef;
                }
            }

            # Create and push the message onto the queue for posting to TSDS
            my $msg = {
                type     => $self->measurement_type,
                time     => $datum_tm,
                interval => $self->interval,
                values   => \%vals,
                meta     => \%meta
            };
            push(@{$self->msg_list}, $msg);
        }
    }
}


=head2 _push_data
    This pushes data messages to TSDS once they've been processed
=cut
sub _push_data {

    my ($self)   = @_;
    my $msg_list = $self->msg_list;
    my $res      = $self->tsds_pusher->push($msg_list);
    #$self->logger->debug(Dumper($msg_list));
    #$self->logger->debug(Dumper($res));

    unless ($res) {
        # If queue is empty and stop flag is set, end event loop
        $self->cv->send() if $self->stop_me;

        # Otherwise clear push timer
        $self->_set_push_w(undef);
        exit(0) if $self->stop_me;
    }
}

1;
