package GRNOC::Simp::Poller::Worker;

use strict;
use Try::Tiny;
use Data::Dumper;
use Moo;
use AnyEvent;
use AnyEvent::SNMP;
use Net::SNMP::XS; # Faster than non-XS
use Redis;
use JSON;

# Raised from 64, Default value
$AnyEvent::SNMP::MAX_RECVQUEUE = 128;

### Required Attributes ###
=head1 public attributes

=over 12

=item group_name

=item instance

=item config

=item logger

=item hosts

=item status_dir

=item oids

=item interval

=back

=cut

has group_name => (
    is => 'ro',
    required => 1
);

has instance => (
    is => 'ro',
    required => 1
);

has config => (
    is => 'ro',
    required => 1
);

has logger => (
    is => 'rwp',
    required => 1
);

has hosts => (
    is => 'ro',
    required => 1
);

has status_dir => (
    is => 'ro',
    required => 1
);

has oids => (
    is => 'ro',
    required => 1
);

has interval => (
    is => 'ro',
    required => 1
);

### Internal Attributes ###
=head1 private attributes

=over 12

=item worker_name

=item is_running

=item need_restart

=item redis

=item retention

=item request_size

=item timeout

=item main_cv

=back

=cut

has worker_name => (
    is => 'rwp',
    required => 0,
    default => 'unknown'
);

has is_running => (
    is => 'rwp',
    default => 0
);

has need_restart => (
    is => 'rwp',
    default => 0 
);

has redis => (
    is => 'rwp'
);

has retention => (
    is => 'rwp',
    default => 5
);

has request_size => (
    is => 'rwp',
    default => 1
);

has timeout => (
    is => 'rwp',
    default => 5
);

has main_cv => (
    is => 'rwp'
);

### Public Methods ###

=head1 methods

=over 12

=cut

=item start

=cut

sub start {

    my ( $self ) = @_;

    $self->_set_worker_name($self->group_name. '[' . $self->instance .']');

    my $logger = GRNOC::Log->get_logger($self->worker_name);
    $self->_set_logger($logger);

    my $worker_name = $self->worker_name;
    $self->logger->error( $self->worker_name." Starting." );

    # Flag that we're running
    $self->_set_is_running( 1 );

    # Change our process name
    $0 = "simp_poller($worker_name)";

    # Setup signal handlers
    $SIG{'TERM'} = sub {
        $self->logger->info($self->worker_name. " Received SIG TERM." );
        $self->stop();
    };

    $SIG{'HUP'} = sub {
        $self->logger->info($self->worker_name. " Received SIG HUP." );
    };

    # Connect to redis
    my $redis_host = $self->config->get( '/config/redis/@host' )->[0];
    my $redis_port = $self->config->get( '/config/redis/@port' )->[0];

    $self->logger->debug($self->worker_name." Connecting to Redis $redis_host:$redis_port." );

    my $redis;

    try {
    # Try to connect twice per second for 30 seconds, 60 attempts every 500ms.
        $redis = Redis->new(
            server    => "$redis_host:$redis_port",
            reconnect => 60,
            every     => 500,
            read_timeout => 2,
            write_timeout => 3,
        );

    } catch {
        $self->logger->error($self->worker_name." Error connecting to Redis: $_" );
    };

    $self->_set_redis( $redis );

    $self->_set_need_restart(0);

    $self->logger->debug( $self->worker_name . ' Starting SNMP Poll loop.' );

    $self->_connect_to_snmp();
    if (! scalar(keys %{$self->hosts}) ) {
        $self->logger->debug("No hosts found for " . $self->worker_name);
    } else {
        $self->logger->debug($self->worker_name . ' hosts: "' . (join '", "', (keys %{$self->hosts})) . '"');
    }

    # Start AnyEvent::SNMP's max outstanding requests window equal to the total
    # number of requests this process will be making. AnyEvent::SNMP will scale from there
    # as it observes bottlenecks
    if ( scalar(@{$self->oids})  && scalar(keys %{$self->hosts}) ) {
        AnyEvent::SNMP::set_max_outstanding( scalar(keys %{$self->hosts}) * scalar(@{$self->oids}) );
    } 
    else {
        $self->logger->error("Hosts or OIDs were not defined!");
        return;
    }

    

    $self->{'collector_timer'} = AnyEvent->timer(
        after => 10,
        interval => $self->interval,
        cb => sub {
            $self->_collect_data();
            AnyEvent->now_update;
        }
    );

    # Let the magic happen
    my $cv = AnyEvent->condvar;
    $self->_set_main_cv($cv);
    $cv->recv;
    $self->logger->error("Exiting");
}


=item stop

=back

=cut

sub stop {
    my $self = shift;
    $self->logger->error("Stop was called");
    $self->main_cv->send();
}


sub _poll_cb {

    my $self = shift;
    my %params = @_;

    # Set params
    my $session     = $params{'session'};
    my $host        = $params{'host'};
    my $host_name   = $params{'host_name'};
    my $req_time    = $params{'timestamp'};
    my $reqstr      = $params{'reqstr'};
    my $main_oid    = $params{'oid'};
    my $context_id  = $params{'context_id'};

    my $redis       = $self->redis;
    my $id          = $self->group_name;
    my $data        = $session->var_bind_list();
    my $timestamp   = $req_time;

    my $ip          = $host->{'ip'};
    my $poll_id     = $host->{'poll_id'};
    
    $self->logger->debug("_poll_cb running for $host_name: $main_oid");
    if ( defined($context_id) ) {
        delete $host->{'pending_replies'}->{$main_oid . "," . $context_id};
    } else {
        delete $host->{'pending_replies'}->{$main_oid};

    }

    my @values;
    # It's possible we didn't get anything back from this OID for some reason, but we still need
    # to advance the "pending_replies" since we did complete the action
    if ( !defined $data ) {

        # OID with no data added to hash of failed OIDs
        $host->{failed_oids}{$main_oid} = {
            error       => "OID_DATA_ERROR",
            timestamp   => time()
        };

        if ( defined $context_id ) {
            $host->{failed_oids}{$main_oid}{context} = $context_id;
        }

        my $error = $session->error();
        $self->logger->error("Host/Context ID: $host_name " . (defined($context_id) ? $context_id : '[no context ID]'));
        $self->logger->error("Group \"$id\" failed: $reqstr");
        $self->logger->error("Error: $error");

    } else {
        for my $oid ( keys %$data ) {
            push(@values, "$oid," .  $data->{$oid});
        }   
    }

    my $expires = $timestamp + $self->retention;

    my $group_interval = $self->group_name . "," . $self->interval;

    try {

        $redis->select(0);

        my $key = $host_name . "," . $self->worker_name . ",$timestamp";

        $redis->sadd($key, @values) if (@values);

        if ( scalar(keys %{$host->{'pending_replies'}}) == 0 ) {

            #$self->logger->error("Received all responses!");
            $redis->expireat($key, $expires);

            # Our poll_id to time lookup
            $redis->select(1);

            my $node_base_key       = $host_name . "," . $self->group_name;
            my $ip_base_key         = $ip . "," . $self->group_name;
            my $poll_timestamp_val  = $poll_id . "," . $timestamp;
            my $host_name_key       = $node_base_key . "," . $poll_id;
            my $ip_key              = $ip_base_key . "," . $poll_id;

            $redis->set($host_name_key, $key);
            $redis->set($ip_key, $key);

            # ...and expire
            $redis->expireat($host_name_key, $expires);
            $redis->expireat($ip_key, $expires);

            $redis->select(0);
            #$self->logger->error(Dumper($host->{'group'}{$self->group_name}));

            if ( $self->hosts->{$host_name} && defined($host->{'host_variable'}) ) {

                $self->logger->debug("Adding host variables for $host_name");

                my %add_values = %{$host->{'host_variable'}};

                foreach my $name ( keys %add_values ) {

                    my $sanitized_name = $name;
                    $sanitized_name =~ s/,//g; # We don't allow commas in variable names

                    my $str = 'vars.' . $sanitized_name . "," . $add_values{$name}->{'value'};

                    $redis->select(0);
                    $redis->sadd($key, $str);
                }

                $redis->select(3);
                $redis->hset($host_name, "vars", $group_interval);
                $redis->hset($ip, "vars", $group_interval);
            }

            # ...and the current poll_id lookup
            $redis->select(2);
            $redis->set($node_base_key , $poll_timestamp_val);
            $redis->set($ip_base_key, $poll_timestamp_val);

            # ...and expire
            $redis->expireat($node_base_key, $expires);
            $redis->expireat($ip_base_key, $expires);

            # Increment the actual reference instead of local var
            $host->{'poll_id'}++;
        }


        $redis->select(3);
        $redis->hset($host_name, $main_oid, $group_interval);
        $redis->hset($ip, $main_oid, $group_interval);

        # Change back to the primary db...
        $redis->select(0);
        # Complete the transaction

    } catch {
        $redis->select(0);
        $self->logger->error($self->worker_name. " $id Error in hset for data: $_" );
    };

    AnyEvent->now_update;
}


sub _connect_to_snmp {
    my $self = shift;
    my $hosts = $self->hosts;

    # Build the SNMP object for each host of interest
    foreach my $host_name ( keys %$hosts ) {

        my $host = $hosts->{$host_name};

        my ($snmp,$error);
        $host->{snmp_errors} = {};

        # SNMP V2C
        if ( !defined($host->{'snmp_version'}) || $host->{'snmp_version'} eq '2c' ) {
            
            # Send an error if v2c and missing a community
            if ( !$host->{community} ) {
                $self->logger->error("SNMP is v2c, but no community is defined for $host_name!");
                $host->{snmp_errors}{community} = {
                    time   => time,
                    error  => "No community was defined for $host"
                };
            }

            ($snmp, $error) = Net::SNMP->session(
                -hostname         => $host->{'ip'},
                -community        => $host->{'community'},
                -version          => 'snmpv2c',
                -timeout          => $self->timeout,
                -maxmsgsize       => 65535,
                -translate        => [-octetstring => 0],
                -nonblocking      => 1,
                -retries          => 5
            );

            $self->{'snmp'}{$host_name} = $snmp;

        # SNMP V3
        } elsif ( $host->{'snmp_version'} eq '3' ) {

            if ( !defined($host->{'group'}{$self->group_name}{'context_id'}) ) {

                ($snmp, $error) = Net::SNMP->session(
                    -hostname         => $host->{'ip'},
                    -version          => '3',
                    -timeout          => $self->timeout,
                    -maxmsgsize       => 65535,
                    -translate        => [-octetstring => 0],
                    -username         => $host->{'username'},
                    -nonblocking      => 1,
                    -retries          => 5
                );

                $self->{'snmp'}{$host_name} = $snmp;

            } else {

                foreach my $ctxEngine ( @{$host->{'group'}{$self->group_name}{'context_id'}} ) {

                    ($snmp, $error) = Net::SNMP->session(
                        -hostname         => $host->{'ip'},
                        -version          => '3',
                        -timeout          => $self->timeout,
                        -maxmsgsize       => 65535,
                        -translate        => [-octetstring => 0],
                        -username         => $host->{'username'},
                        -nonblocking      => 1,
                        -retries          => 5
                    );

                    $self->{'snmp'}{$host_name}{$ctxEngine} = $snmp;
                }
            }

        # Send an error if the SNMP version is invalid
        } else {
            $self->logger->error("Invalid SNMP Version for SIMP");
            $host->{snmp_errors}{version} = {
                time  => time,
                error => $error
            };
        }

        # Send an error if the SNMP session object for the host wasn't created
        if ( !$snmp ) {
            $self->logger->error("Error creating SNMP Session: $error");
            $host->{snmp_errors}{session} = {
                time  => time,
                error => $error
            };
        }

        my $host_poll_id;

        try {

            $self->redis->select(2);
            $host_poll_id = $self->redis->get($host_name . ",main_oid");
            $self->redis->select(0);

            if( !defined($host_poll_id) ) {
                $host_poll_id = 0;
            }

        } catch {
            $self->redis->select(0);
            $host_poll_id = 0;
            $self->logger->error("Error fetching the current poll cycle id from redis: $_");
            $host->{snmp_errors}{redis} = {
                time  => time,
                error => "Error fetching the current poll cycle id from redis $_"
            }; 
        };

        $host->{'poll_id'} = $host_poll_id;
        $host->{'pending_replies'} = {};
        $host->{'failed_oids'} = {};

        $self->logger->debug($self->worker_name . " assigned host " . $host_name);
    }
}


sub _write_mon_data {

    my $self = shift;
    my $host = shift;
    my $name = shift;

    # Set dir for writing status files to status_dir defined in simp-poller.pl
    my $mon_dir = $self->status_dir . $name . '/';

    # Checks if $mon_dir exists or creates it
    unless (-e $mon_dir) {
        # Note: If the dir can't be accessed due to permissions, writing will fail
        $self->logger->error("Could not find dir for monitoring data: $mon_dir");
        return;
    }

    # Add filename to the path for writing
    $mon_dir .= $self->group_name . "_status.json";
       
    my %mon_data = (
        timestamp   => time(),
        failed_oids => scalar($host->{failed_oids}) ? $host->{failed_oids} : '',
        snmp_errors => scalar($host->{snmp_errors}) ? $host->{snmp_errors} : '',
        config      => $self->config->{config_file},
        interval    => $self->interval
    );
    $self->logger->debug(Dumper(\%mon_data));
        
    $self->logger->debug("Writing status file $mon_dir"); 

    # Write out the status file
    if ( open(my $fh, '>:encoding(UTF-8)', $mon_dir) ) {
       print $fh JSON->new->pretty->encode(\%mon_data);
       close($fh) || $self->logger->error("Could not close $mon_dir");
    }
    else {
        $self->logger->error("Could not open " . $mon_dir);
        return;
    }

    $self->logger->debug("Writing completed for status file $mon_dir");
}


sub _collect_data {

    my $self = shift;

    my $hosts         = $self->hosts;
    my $oids          = $self->oids;
    my $timestamp     = time;

    $self->logger->debug("----  START OF POLLING CYCLE FOR: \"" . $self->worker_name . "\"  ----" );

    $self->logger->debug($self->worker_name . " " . $self->group_name . " with " . scalar(keys %$hosts) . " hosts and " . scalar(@$oids) . " oids per hosts, max outstanding scaled to " . $AnyEvent::SNMP::MAX_OUTSTANDING, " queue is " . $AnyEvent::SNMP::MIN_RECVQUEUE . " to " . $AnyEvent::SNMP::MAX_RECVQUEUE);

    for my $host_name (keys %$hosts) {

        my $host = $hosts->{$host_name};
        
        # Write mon data out for the host's last poll cycle
        $self->_write_mon_data($host, $host_name);
        # Reset the failed OIDs after writing out the status
        $host->{failed_oids} = {};

        # Used to stagger each request to a specific host, spread load.
        # This does mean you can't collect more OID bases than your interval
        my $delay = 0;

        # Log error and skip collections for the host if it has an OID key in pending response with a val of 1
        if ( scalar(keys %{$host->{'pending_replies'}}) > 0 ) {
            $self->logger->error("Unable to query device " . $host->{'ip'} . ":" . $host_name . " in poll cycle for group: " . $self->group_name . " remaining oids = " . Dumper($host->{'pending_replies'}));
            next;
        }

        my $snmp_session  = $self->{'snmp'}{$host_name};
        my $snmp_contexts = $host->{'group'}{$self->group_name}{'context_id'};

        if ( !defined($snmp_session) ) {
            $self->logger->error("No SNMP session defined for $host_name");
            next;
        }
        
        for my $oid (@$oids) {
            
            # Log an error if the OID is failing
            if ( exists $host->{'failed_oids'}->{$oid} ) {
                $self->logger->error($host->{'failed_oids'}->{$oid}->{'error'});
            }

            my $reqstr = " $oid -> $host->{'ip'} ($host_name)";
            #$self->logger->debug($self->worker_name ." requesting ". $reqstr); 

            # Iterate through the the provided set of base OIDs to collect
            my $res;
            my $res_err = "Unable to issue get_table for group \"" . $self->group_name . "\"";

            # V3 without Context and V2C (They work the same way)
            if ( $host->{'snmp_version'} eq '2c' || ! defined($snmp_contexts) ) {
                $self->logger->debug($self->worker_name . " requesting " . $reqstr);
                $res = $snmp_session->get_table(
                    -baseoid         => $oid,
                    -maxrepetitions  => $self->request_size,
                    -delay           => $delay++,
                    -callback        => sub {
                        my $session = shift;
                        $self->_poll_cb( 
                            host        => $host,
                            host_name   => $host_name,
                            timestamp   => $timestamp,
                            reqstr      => $reqstr,
                            oid         => $oid,
                            session     => $session
                        );
                    }
                );

                if ( !$res ) {
                    # Add oid and info to failed_oids if it failed during request
                    $host->{'failed_oids'}{$oid} = {
                        error       => "OID_REQUEST_ERROR",
                        description => $snmp_session->error(),
                        timestamp   => time()
                    };
                    
                    $self->logger->error("$res_err: " . $snmp_session->error());

                } else {
                    # Add to pending replies if request is successful
                    $host->{'pending_replies'}->{$oid} = 1;
                }

            # V3 with Context
            } elsif ( defined $snmp_contexts ) {
                # For each context engine specified for the group, also use the context
                # specific snmp_session established in _connect_to_snmp
                foreach my $ctxEngine (@$snmp_contexts) {
                    $host->{'pending_replies'}->{$oid . "," . $ctxEngine} = 1;
                    $res = $snmp_session->{$ctxEngine}->get_table(
                        -baseoid            => $oid,
                        -maxrepetitions     => $self->request_size,
                        -contextengineid    => $ctxEngine,
                        -delay              => $delay++,
                        -callback           => sub {
                            my $session = shift;
                            $self->_poll_cb( 
                                host        => $host,
                                host_name   => $host_name,
                                timestamp   => $timestamp,
                                reqstr      => $reqstr,
                                oid         => $oid,
                                context_id  => $ctxEngine,
                                session     => $session
                            );
                            $self->logger->debug("Created _poll_cb for $oid");
                        }
                    );

                    if ( !$res ) {
                        # Add oid and info to failed_oids if it failed during request
                        $host->{'failed_oids'}{$oid} = {
                            error       => "OID_REQUEST_ERR",
                            description => $snmp_session->error(),
                            timestamp   => time()
                        };

                        $self->logger->error("$res_err with context $ctxEngine: " . $snmp_session->{$ctxEngine}->error());

                    } else {
                        # Add to pending replies if request is successful
                        $host->{'pending_replies'}->{$oid . "," . $ctxEngine} = 1;
                    } 

                }

            # Shouldn't get here, this could be misconfig?
            } else {
                $self->logger->error("Error collecting data - unsupported configuration for $host_name " . $self->group_name);
            }

        }
        $self->logger->debug(Dumper($host));
        
    }
    $self->logger->debug("----  END OF POLLING CYCLE FOR: \"" . $self->worker_name . "\"  ----" );
}

1;
