package GRNOC::Simp::TSDS::Worker;

# Documentation commands in Perl Pod format.
# see: https://perldoc.perlorg/perlpod.

=head1 GRNOC::SIMP::TSDS::Worker
    Workers that push data requested from Simp::Comp to a TSDS DB.
    A worker process will perform this on a set interval cycle.
    The data type is set on creation and is immutable.
    Input is requested from GRNOC::Simp::Comp via AMQP.
    Output is pushed to a TSDS DB given the config defined in GRNOC::Simp:TSDS.
=cut

use strict;
use warnings;

use Data::Dumper;
use Time::HiRes qw(gettimeofday tv_interval usleep);
use List::MoreUtils qw(any natatime);
use Moo;

use GRNOC::RabbitMQ::Client;
use GRNOC::RabbitMQ::Dispatcher;
use GRNOC::RabbitMQ::Method;
use GRNOC::Monitoring::Service::Status;
use GRNOC::Simp::TSDS;
use GRNOC::Simp::TSDS::Pusher;

=head2 PUBLIC ATTRIBUTES
=over 4 
=item name
=item id
=item hosts
=item composite
=item measurement
=item interval
=item exclusions
=item filter
=item logger
=item rmq_config
=item tsds_config
=item status_dir
=item stagger
=back
=cut
has name        => (is => 'ro',  required => 1);
has id          => (is => 'ro',  required => 1);
has hosts       => (is => 'rwp', required => 1);
has composite   => (is => 'rwp', required => 1);
has measurement => (is => 'rwp', required => 1);
has interval    => (is => 'rwp', required => 1);
has exclusions  => (is => 'rwp', default => sub { [] });
has filter      => (is => 'rwp', default => sub { {} });
has logger      => (is => 'rwp', required => 1);
has rmq_config  => (is => 'rwp', required => 1);
has tsds_config => (is => 'rwp', required => 1);
has status_dir  => (is => 'rwp', required => 1);
has stagger     => (is => 'rwp', required => 1);

=head2 PRIVATE ATTRIBUTES
=over 4
=item simp_client
=item tsds_pusher
=item poll_w
=item push_w
=back
=cut
has simp_client => (is => 'rwp');
has tsds_pusher => (is => 'rwp');
has poll_w      => (is => 'rwp');
has push_w      => (is => 'rwp');
has messages    => (is => 'rwp', default => sub { [] });

=head2 start()
    This method is called by the Simp.TSDS master to start up the worker
    We loop on _run() where the event loop is entered and exited.
    The loop will only stop once the process is terminated.
    Once _run() is called, we only re-enter the loop when RMQ disconnects.
    This allows us to reconnect by restarting the worker processes.
=cut
sub start {
    
    my $self = shift;

    # Change the process name
    $0 = sprintf("simp_tsds(%s) [%s]", $self->name, $self->id);

    $self->logger->info($0);

    # Setup signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info($0 . " - Received SIG TERM.");
        exit(1);
    };
    $SIG{'INT'} = sub {
        $self->logger->info($0 . " - Received SIG INT.");
        exit(1);
    };
    $SIG{'HUP'} = sub {
        $self->logger->info($0 . " - Received SIG HUP.");
        exit(1);
    };


    # Add worker PID to the path passed in by master
    $self->_set_status_dir(sprintf("%s/%s-%s-", $self->status_dir, $$, $self->name));

    # Initialize status file
    my $res = write_service_status(
        path         => $self->status_dir,
        service_name => $0,
        error        => 0,
    );

    if ( $res ) {
        $self->logger->debug("Created status file in ".$self->status_dir);
    } else {
        $self->logger->error(sprintf("ERROR: Worker %s was unable to create a status file", $self->name));
    }

    # I'm not sure if this is a good idea or not. The intent here is to make it so that
    # if the stagger time would bring us to within 10% of the next interval, we truncate it
    # down to 80%. We don't want the timer firing on -exactly- the interval since we will
    # then be bordering on whether we're within T-now or T-now+1 based on any drift
    my $now        = time();
    my $start_time = $now + $self->stagger;

    $self->logger->debug($self->name ." now = " . time() . " -- next start = $start_time  -- stagger = " . $self->stagger . " -- interval = " . $self->interval);

    # e.g 60s interval, starting at T=57s, move backward to starting at T=51s
    if ($start_time % $self->interval > $self->interval * 0.9){
    $start_time -= $self->interval * 0.1;
    }

    # e.g. 60s interval, starting at T=3s, move forward to starting at T=9s
    elsif ($start_time % $self->interval < $self->interval * 0.1){
    $start_time += $self->interval * 0.1;
    }

    my $sleep_stagger = $start_time - $now;
    $sleep_stagger = 0 if $sleep_stagger < 0;

    $self->logger->debug($self->name . " sleeping for " . $sleep_stagger . " seconds to stagger");
    sleep($sleep_stagger);

    # This startup loop will catch when a worker exits the event loop without termination
    # In turn, we can handle connection errors here and/or restart the worker's main processes
    while (1) {

        $self->logger->info($self->name." Starting...");
        $self->_run();

        $self->logger->error($self->name." Not connected to RabbitMQ, retrying after 1 interval (".$self->interval."s)");
        sleep $self->interval;
    }
        
}


=head2 run
    This kicks off the worker processes and enters the event loop.
    We only enter the event loop when RabbitMQ is connected.
    On an error the process exits or _run() is called again by the start() loop.
=cut
sub _run {
    my ($self) = @_;

    # Connect and set the RabbitMQ Client
    # The worker will be locked on this step until Rabbitmq has connected
    $self->_setup_client();

    # We don't want to perform any other operations until RabbitMQ is connected
    # Once it is, we create the pusher and enter the event loop.
    if ($self->simp_client && $self->simp_client->connected) {

        $self->logger->info($self->name.' Entering event loop');
    
        # Create and set the Simp.TSDS Pusher instance for the worker
        $self->_setup_pusher();

        # Set the worker properties and event timer, entering the event loop
        $self->_setup_worker();
    }
}


=head2 _setup_client()
    This will connect the worker's RabbitMQ Client.
    If the worker fails to connect on the first try, the event loop will not start.
    We continue the _run() loop until connected.
=cut
sub _setup_client {

    my $self  = shift;

    my $client = GRNOC::RabbitMQ::Client->new(
        host     => $self->rmq_config->{'ip'},
        port     => $self->rmq_config->{'port'},
        user     => $self->rmq_config->{'user'},
        pass     => $self->rmq_config->{'password'},
        exchange => 'Simp',
        topic    => 'Simp.Comp',
    );
    $self->_set_simp_client($client);

    unless ($client && $client->connected) {
        $self->logger->error($self->name.' Could not connect to RabbitMQ');
    }
    else {
        $self->logger->debug($self->name.' RabbitMQ Client connected successfully');
    }
}


=head2 _setup_pusher()
    This will create and set the Simp.TSDS Pusher for the worker
=cut
sub _setup_pusher {

    my $self = shift;

    my $pusher = GRNOC::Simp::TSDS::Pusher->new(
        logger      => $self->logger,
        worker_name => $self->name,
        tsds_config => $self->tsds_config,
    );

    if (!$pusher) {
        $self->logger->error($self->name." Could not create the Simp.TSDS Pusher, please check the config");
    }
    else {
        $self->logger->info($self->name." Created the Simp.TSDS Pusher");
        $self->_set_tsds_pusher($pusher);
    }
}

=head2 _setup_worker()
    This will start the main Worker processes and enter the timed event loop.
    The loop will exit if RabbitMQ disconnects or the process is terminated.
    The timer will call to process and push data whenever it's not requesting it.
=cut
sub _setup_worker {

    my ($self) = @_;

    # Get the composite name to use as a RabbitMQ method
    my $composite = $self->composite;

    # Create polling timer for event loop
    while (1) {

        # For timing
        my $cycle_start = gettimeofday();

        # For query
        my $now = time();

        # Exit the event loop but don't exit the process when RMQ disconnects
        unless ($self->simp_client && $self->simp_client->connected) {
            $self->logger->error($self->name." Disconnected from RabbitMQ, restarting");
            return;
        }
        
        # Pull data for each host from Comp
        for my $host (@{$self->hosts}) {
            
            $self->logger->info($self->name." Processing ".$host->{'name'});

            # Set the arguments to pass to Simp::Comp
            # Always search one interval behind to avoid a race condition with Simp::Poller
            my %args = (
                node    => $host->{'name'},
                period  => $self->interval,
                time    => $now - $self->interval
            );
            
            # if we're trying to only get a subset of values out of
            # simp, add those arguments now. This presumes that SIMP
            # and SIMP-collector have been correctly configured to have
            # the data available validity is checked for earlier
            # in Master
            if (defined($self->filter->{'name'})) {
                $args{$self->filter->{'name'}} = $self->filter->{'value'};
            }
            
            # to provide some degree of backward compatibility,
            # we only put this field on if we need to:
            if (scalar(@{$self->exclusions}) > 0) {
                $args{'exclude_regexp'} = $self->exclusions;
            }
            
            # Add a request for the composite method from RabbitMQ
            # We pass the args hash from above to the method
            my $start_query = gettimeofday();
            my $res         = $self->simp_client->$composite(%args);
            my $end_query   = gettimeofday();

            $self->logger->debug(sprintf(
                "%s Received %s (%ss)",
                $self->name,
                $host->{'name'},
                tv_interval([$start_query],[$end_query])
            ));

            $self->_process_data($res, $host);
        }	

        $self->_push_data();

        my $cycle_end = gettimeofday;
        my $elapsed   = tv_interval([$cycle_start], [$cycle_end]);
        my $diff      = $self->interval - $elapsed;

        # Clear internal buffer
        $self->_set_messages([]);

        if ($diff > 0){	    
            $self->logger->info($self->name . " sleeping $diff seconds until next cycle...");
            usleep($diff * 1000 * 1000);
        }
        else {
            $self->logger->warn($self->name . " !! Took too long on previous cycle, would have slept for $diff seconds");
        }
    }
}


=head2 _process_data
    This will process data from Comp into TSDS-friendly messages.
    All data is processed before it can be posted to TSDS.
    Metadata and Value fields are separated here.
=cut
sub _process_data {
    my ($self, $res, $host) = @_;

    # Drop out if we get an error from Comp
    if (!defined($res) || $res->{'error'}) {
        $self->logger->error($self->name." Comp error getting data for $host: ".GRNOC::Simp::TSDS::error_message($res));
        return;
    }

    # Take data from Comp by node and process for posting to TSDS
    for my $node_name (keys %{$res->{'results'}}) {

        my $data = $res->{'results'}->{$node_name};

        # Process every data object/hash for the node
        for my $datum (@$data) {

            my %vals;
            my %meta;
            my $datum_tm;

            # When required_values are empty, skip the data
            next if (any { !defined($datum->{$_}) && !defined($datum->{"*$_"}) } @{$host->required_values});

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
                type     => $host->measurement,
                time     => $datum_tm,
                interval => $self->interval,
                values   => \%vals,
                meta     => \%meta
            };
            push(@{$self->messages}, $msg);
        }
    }
}


=head2 _push_data
    This pushes data messages to TSDS once they've been processed.
    We call this method as a callback from within the event loop.
=cut
sub _push_data {

    my ($self)   = @_;

    $self->logger->debug($self->name . " Sending " . scalar(@{$self->messages}) . " messages");

    my $iterator = natatime(100, @{$self->messages});
    my $last_failure;
    my $res;
    while (my @block = $iterator->()) {
        $res = $self->tsds_pusher->push(\@block);
        if ( defined($res) && $res->{'error'} ) { 
            $last_failure = $res;
        }
    }
    # Last failure will catch cases where an error occurs in a middle block of the iterator
    # which would otherwise be overwritten by a subsequent 'okay' block
    $self->_write_push_status(( defined($last_failure) ? $last_failure : $res ));

}

# Writes the results of a push action, $res, to a status file, /var/lib/grnoc/simp-tsds/($name)status.txt
sub _write_push_status {
    my $self  = shift; 
    my $res = shift;

    my $path = $self->status_dir;
    my $error = (defined($res) && $res->{'error'}) ? 1 : 0;
    my $error_txt = ($error eq 1) ? $res->{'error'} : "";
 
    my $write_res = write_service_status(
        path      => $path,
        error     => $error + 0,
        error_txt => $error_txt,
    );
    if (!$write_res) {
        $self->logger->error(sprintf("ERROR: Worker %s was unable to write a status file after push to tsds", $self->name));
    }

}

1;
