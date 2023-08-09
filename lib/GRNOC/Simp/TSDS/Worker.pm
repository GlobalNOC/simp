package GRNOC::Simp::TSDS::Worker;

#---------------------------------------------------
# IMPORTS
#---------------------------------------------------
use strict;
use warnings;

use Moo;
use Try::Tiny;
use Data::Dumper;
use JSON::XS qw(encode_json);
use Time::HiRes qw(gettimeofday tv_interval usleep);
use List::MoreUtils qw(any natatime);

use GRNOC::RabbitMQ::Client;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Method;
use GRNOC::WebService::Client;


#---------------------------------------------------
# PUBLIC ATTRIBUTES
#---------------------------------------------------
has id          => (is => 'ro', required => 1);
has requests    => (is => 'ro', required => 1);
has logger      => (is => 'ro', required => 1);
has rmq_config  => (is => 'ro', required => 1);
has tsds_config => (is => 'ro', required => 1);
has unsent      => (is => 'ro', default => sub { [] });


#---------------------------------------------------
# PRIVATE ATTRIBUTES
#---------------------------------------------------
has rabbitmq  => (is => 'rwp');
has tsds      => (is => 'rwp');
has data      => (is => 'rwp', default => sub { [] });
has status    => (is => 'rwp', default => sub { {} });
has push_size => (is => 'rwp', default => 100);


#---------------------------------------------------
# PUBLIC METHODS
#---------------------------------------------------
# This method is called by GRNOC::Simp::TSDS to run the worker.
# It will request data from Simp::Comp and push results to TSDS.
# Returns a status hash to GRNOC::Simp::TSDS.
sub run {
    my ($self) = @_;

    # Track the start time of the run
    my $cycle_start = gettimeofday();

    # Set up the worker process
    # Return early if RabbitMQ or TSDS connection fails.
    return $self->status unless ($self->_setup());

    # Process requests by gathering data from Simp::Comp and preparing it for TSDS.
    $self->_process_requests();

    # Push the data for the processed requests.
    $self->_push_data($self->data, 'new');

    # Try pushing unsent data received from the Simp::TSDS unsent cache.
    $self->_push_data($self->unsent, 'unsent');

    # Calculate and log the run time for the worker.
    # Rounded to 3 decimal places.
    $self->status->{'duration'} = sprintf("%.3f", tv_interval([$cycle_start], [gettimeofday]));

    # Return the status data cache
    return $self->status;
}

#---------------------------------------------------
# PRIVATE METHODS
#---------------------------------------------------
# A wrapper method for various methods that set up the worker.
# Sets the process name, status cache, and RabbitMQ and TSDS clients.
sub _setup {
    my ($self) = @_;

    # Change the process name.
    $0 = sprintf("simp_tsds [%s]", $self->id);

    # Set up the status data cache.
    $self->_setup_status();

    # Exit setup if there are no requests to process.
    return unless (scalar(@{$self->requests}));

    # Set up the connection to RabbitMQ, exit if connection fails.
    return unless ($self->_setup_rabbitmq());

    # Set up the connection to the TSDS API
    # Do not return failed state on failed connection.
    # Data messages can still be produced and cached for resending.
    $self->_setup_tsds();

    return 1;
}

# Method that initializes the status cache.
# NOTE: Must run before any other methods.
sub _setup_status {
    my ($self) = @_;
    $self->logger->debug("Setting up status cache");

    my %status = (
        'errors'   => 0,        # Worker had ANY errors
        'rabbitmq' => 0,        # Worker can't connect to RabbitMQ
        'tsds'     => 0,        # Worker can't connect to TSDS
        'duration' => -1,       # Worker run duration
        'name'     => $0,       # Worker process name
        'time'     => time(),   # Timestamp
        'requests' => {},       # Error data for individual request IDs
        'unsent'   => [],       # Data messages not pushed to TSDS
    );

    $self->_set_status(\%status);
    $self->logger->debug("Set the status cache");
}

# Checks the connection state of the cached RabbitMQ Client.
# Returns true when connected.
sub _check_rabbitmq {
    my ($self) = @_;
    unless ($self->rabbitmq && $self->rabbitmq->connected) {
        $self->logger->error('Not connected to RabbitMQ');
        return 0;
    }
    $self->logger->debug('RabbitMQ Client is connected');
    return 1;
}

# Creates a new RabbitMQ Client and caches it.
# Returns true on successful creation and connection.
sub _setup_rabbitmq {
    my ($self) = @_;
    $self->logger->debug("$0: Setting up RabbitMQ client");

    # Create a new RabbitMQ client and cache it.
    my $client = GRNOC::RabbitMQ::Client->new(
        host     => $self->rmq_config->{'ip'},
        port     => $self->rmq_config->{'port'},
        user     => $self->rmq_config->{'user'},
        pass     => $self->rmq_config->{'password'},
        exchange => 'Simp',
        topic    => 'Simp.Comp',
        exlusive => 0,
    );
    $self->_set_rabbitmq($client);
    $self->logger->debug("$0: Set the RabbitMQ client");

    # Check the connection status of RabbitMQ
    my $connected = $self->_check_rabbitmq();

    # Update the status if the connection failed.
    unless ($connected) {
        $self->status->{'errors'}   = 1;
        $self->status->{'rabbitmq'} = 1;
    }

    return $connected;
}

# Checks the connection state of the cached TSDS Push API.
# Returns true when connected.
sub _check_tsds {
    my ($self) = @_;
    unless ($self->tsds->help()) {
        $self->logger->error("$0: Not connected to TSDS");
        return 0;
    }
    $self->logger->debug("$0: TSDS client is connected");
    return 1;
}

# Creates and sets the TSDS GRNOC::WebServiceClient for the worker.
# The TSDS client is used to push data to a TSDS API push.cgi endpoint.
sub _setup_tsds {
    my ($self) = @_;
    $self->logger->debug("$0: Setting up TSDS Push API client");

    # Create a new RabbitMQ client and cache it.
    my $tsds = GRNOC::WebService::Client->new(
        url     => $self->tsds_config->{'url'},
        uid     => $self->tsds_config->{'user'},
        passwd  => $self->tsds_config->{'password'},
        realm   => $self->tsds_config->{'realm'},
        usePost => 1,

        # DEPRECATED REMOTE SERVICE CONFIGS
        # urn => $self->tsds_config->{'urn'},
        # service_cache_file => SERVICE_CACHE_FILE,
    );
    $self->_set_tsds($tsds);
    $self->logger->debug("$0: Set the TSDS Push API client");

    # Set the push size or use the default.
    my $push_size = $self->tsds_config->{'push_size'};
    $self->_set_push_size($push_size) if ($push_size);

    # Update the status if connection failed.
    unless ($self->_check_tsds()) {
        $self->status->{'errors'} = 1;
        $self->status->{'tsds'}   = 1;
    }
}

# Helper function for processing response errors from RabbitMQ and TSDS.
sub _get_response_error {
    my ($self, $type, $id, $response) = @_;

    my $error;
    if (!defined($response)) {
        $error  = sprintf('%s: No response from %s for %s', $0, $type, $id);
        $error .= sprintf(' - ERROR="%s"', $!) if (defined($!) && ($! ne ''));
    }
    elsif (defined($response->{'error'})) {
        $error  = sprintf('%s: Error response from %s for %s', $0, $type, $id);
        $error .= sprintf(' - ERROR_CODE="%s"', $response->{'error'});

        if (defined $response->{'error_text'}) {
            $error .= sprintf(' - ERROR_TEXT="%s"', $response->{'error_text'});
        }
        $error .= sprintf(' - ERROR="%s"', $!) if (defined($!) && ($! ne ''));
        $error .= sprintf(' - STACK="%s"', $@) if (defined($@) && ($@ ne ''));
    }
    $self->logger->error($error) if ($error);

    return $error
}

# Reads requests and gets data from the RabbitMQ Simp.Comp queue.
# Data returned from RabbitMQ is reformatted for TSDS and cached.
sub _process_requests {
    my ($self) = @_;
    $self->logger->info(sprintf("%s: Processing %s data requests", $0, scalar(@{$self->requests})));

    # Current time for queries
    my $now = time();

    # Queries to be sent to Simp.Comp
    my @queries;

    # Gather data from Simp::Comp for each request
    for my $request (@{$self->requests}) {
        $self->logger->debug(sprintf("%s: getting %s for %s", $0, $request->{'composite'}, $request->{'node'}));

        # Initialize status data for the request with error flags.
        # There are flags for each step, and a general error(s) flag.
        $self->status->{'requests'}{$request->{'id'}} = {
            'errors'   => 0,
            'rabbitmq' => 0,
            'encoding' => 0,
            'tsds'     => 0
        };

        # Set the query to pass to Simp::Comp via the Simp.Comp queue
        # Always search one interval behind to avoid a race condition with Simp::Poller
        my %query = (
            node      => $request->{'node'},
            interval  => $request->{'interval'},
            composite => $request->{'composite'},
            time      => $now - $request->{'interval'}
        );
        
        # Apply filters to the Simp.Comp RabbitMQ query
        if (defined($request->{'filter'}{'name'})) {
            $query{$request->{'filter'}{'name'}} = $request->{'filter'}{'value'};
        }
        
        # Apply exclusions to the Simp.Comp RabbitMQ query
        # NOTE: This is done rarely for backwards-compatiblity
        if (defined($request->{'exclusions'}) && scalar(@{$request->{'exclusions'}}) > 0) {
            $query{'exclude_regexp'} = $request->{'exclusions'};
        }

        # Add the query params to our array of queries to request
        push(@queries, \%query);
    }

    # Time the query
    my $query_start = gettimeofday();

    # Ask Simp::Comp to provide data via the Simp.Comp queue in RabbitMQ
    # The RabbitMQ method name provides the composite to Simp::Comp
    my %args = ('requests' => \@queries);
    my $response = $self->rabbitmq->get(%args);

    # Finish timing the request to Simp.Comp
    my $query_end   = gettimeofday();
    my $query_time  = tv_interval([$query_start], [$query_end]);
    #$self->logger->debug(sprintf("%s: got %s (%ss)", $0, $request->{'id'}, $query_time));

    # Process the data returned by Simp.Comp and cache it.
    $self->_process_data($response);

    $self->logger->debug("$0: Finished processing data from Simp.Comp");
}

# Transforms responses from Simp.Comp into JSON TSDS data messages and caches them.
sub _process_data {
    my ($self, $response) = @_;

    for my $request (@{$self->requests}) {

        $self->logger->debug(sprintf('%s: Processing data response for %s', $0, $request->{'id'}));

        my $id     = $request->{'id'};
        my $node   = $request->{'node'};

        # Get a reference to the request's status trackers.
        my $request_status = $self->status->{'requests'}{$id};

        # Do not process error responses or those without data for the node.
        if ($self->_get_response_error($id, 'RabbitMQ', $response)) {

            # Set the pull and error flags in the status.
            $self->status->{'errors'}      = 1;
            $request_status->{'errors'}    = 1;
            $request_status->{'rabbitmq'}  = 1;
            return;
        }

        # Process every data hash in the response for the node.
        for my $data (@{$response->{'results'}{$node}}) {

            # Skip the data hash when a required field is empty.
            next if (any { !defined($data->{$_}) && !defined($data->{"*$_"}) } @{$request->{'required'}});

            # Build a TSDS Push API data message from the results' data hash.
            # TODO: Simp::Comp should return preformatted TSDS Push data messages
            my $time;
            my %vals;
            my %meta;
            while (my ($k, $v) = each(%$data)) {

                # Get the timestamp
                if ($k eq 'time') {
                    next if !defined($v); # Workaround for 3135:160
                    $time = $v + 0;
                }
                # Keys for metadata start with an asterisk (*)
                elsif ($k =~ /^\*/) {
                    $meta{substr($k, 1)} = $v;
                }
                # Keys for values do not have an asterisk
                else {
                    $vals{$k} = defined($v) ? $v + 0 : undef;
                }
            }

            # Create a data message for the TSDS Push API
            my $message = {
                time     => $time,
                type     => $request->{'measurement'},
                interval => $request->{'interval'},
                values   => \%vals,
                meta     => \%meta
            };

            # Encode the individual data message as a JSON object string.
            # Track and skip messages that could not be encoded as JSON.
            my $message_json;
            try {
                $message_json = encode_json($message);
            } catch {
                $self->logger->error(sprintf("%s: Could not encode data as JSON for %s: %s", $id, $0, $_));
                $self->status->{'errors'}     = 1;
                $request_status->{'errors'}   = 1;
                $request_status->{'encoding'} = 1;
                next;
            };

            # Create a data hash to tie the message to a request ID
            my $data = {
                time    => $time,
                id      => $request->{'id'},
                message => $message_json
            };

            # Cache the data hash
            push(@{$self->data}, $data);
        }
    }
}

# Pushes JSON TSDS data messages to TSDS.
sub _push_data {
    my ($self, $data, $type) = @_;
    $self->logger->debug(sprintf("%s: pushing %s %s messages to TSDS", $0, scalar(@$data), $type));

    # Don't try to push an empty data array.
    return unless (scalar(@$data) > 0);

    # Don't try to push when TSDS is disconnected.
    # Add the data messages to unsent instead.
    if ($self->status->{'tsds'}) {
        push(@{$self->status->{'unsent'}}, @$data);
        return;
    }

    # Iterate over "N at a time" data hashes to batch pushes.
    # The push size comes from config or defaults to 100.
    my $batch_iterator = natatime($self->push_size, @$data);

    # Push each data batch to the TSDS Push API.
    while (my @data_batch = $batch_iterator->()) {

        # Get the batch of data messages only
        my @messages = map { $_->{'message'} } @data_batch;

        # Do not push empty message batches
        next if (scalar(@messages) < 1);

        # Encode the data message JSON batch as a JSON array string
        my $data_json;
        try {
            $data_json = encode_json(\@messages);
        } catch {
            $self->logger->error(sprintf("%s: Could not encode data as JSON: %s", $0, $_));
        };

        # Push the data message JSON array to the TSDS API
        my $response = $self->tsds->add_data(data => $data_json);

        # Get any errors found in the response.
        my $batch_error = $self->_get_response_error('TSDS', 'message batch', $response);

        # Go to the next batch unless there were errors.
        next unless ($batch_error);

        # Retry sending each data message individually if the batch couldn't be pushed.
        # If a specific message can't be sent, keep it to return it to Simp::TSDS.
        # Unsent messages will be retried on the next cycle by Simp::TSDS using retry_push().
        for my $d (@data_batch) {

            # Get the ID and data message JSON from the data hash
            my $id      = $d->{'id'};
            my $message = $d->{'message'};

            # Do not push empty messages
            next unless (defined($message));

            # Get a reference to the request's status trackers.
            my $request_status = $self->status->{'requests'}{$id};

            # Push the data batch JSON to the TSDS API.
            $response = $self->tsds->add_data(data => $message);

            # Get any errors found in the response.
            my $message_error = $self->_get_response_error('TSDS', $id, $response);

            # Track this requests' TSDS push failure.
            if ($message_error) {
                $self->status->{'errors'}   = 1;
                $request_status->{'errors'} = 1;
                $request_status->{'tsds'}   = 1;

                # Allow Simp::TSDS to retry pushing the message if it didn't contain invalid chars.
                # Keep the encoded JSON to prevent re-processing.
                # It will pass it to a worker on the next run, skipping the pull, process, and encode steps.
                unless ($message_error =~ m/only accepts printable characters/g) {
                    push(@{$self->status->{'unsent'}}, $d);
                }
            }
        }
    }
}
1;
