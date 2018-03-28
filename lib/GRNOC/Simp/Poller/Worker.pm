package GRNOC::Simp::Poller::Worker;

use strict;
use Try::Tiny;
use Data::Dumper;
use Moo;
use AnyEvent;
use AnyEvent::SNMP;
use Net::SNMP;
use Redis;
use List::Util qw(max);

use constant DB_MAIN => 0;
use constant DB_POLLID_TO_KEY => 1;
use constant DB_CURRENT_POLLID => 2;
use constant DB_OID_MAP => 3;
use constant DB_MONITORING => 4;

use constant N => 20;

### required attributes ###
=head1 public attributes

=over 12

=item group_name

=item worker_name

=item global_config

=item logger

=item hosts

=item oids

=item poll_interval

=item var_hosts

=back

=cut
has group_name => (is => 'ro',
                   required => 1);

has worker_name => (is => 'ro',
                    required => 1);

has global_config => ( is => 'ro',
                       required => 1 );


has logger => ( is => 'rwp',
                required => 1 );

has hosts => ( is => 'ro',
               required => 1 );

has oids => ( is => 'ro',
              required => 1 );

has poll_interval => ( is => 'ro',
                       required => 1 );

has var_hosts => ( is => 'ro',
                   required => 1 );


### internal attributes ###
=head1 private attributes

=over 12

=item is_running

=item need_restart

=item redis

=item retention

=item max_reps

=item snmp_timeout

=item main_cv

=back

=cut

has is_running => ( is => 'rwp',
                    default => 0 );

has need_restart => (is => 'rwp',
                     default => 0 );

has redis => ( is => 'rwp' );

has retention => (is => 'rwp',
                  default => 5);

has max_reps => (is => 'rwp',
                 default => 1);

has snmp_timeout => (is => 'rwp',
                     default => 5);

has main_cv => (is => 'rwp');

### public methods ###

=head1 methods

=over 12

=cut

=item start

=cut

sub start {

    my ( $self ) = @_;

    my $logger = GRNOC::Log->get_logger($self->worker_name);
    $self->_set_logger($logger);

    my $worker_name = $self->worker_name;
    $self->logger->error( $self->worker_name." Starting." );

    # flag that we're running
    $self->_set_is_running( 1 );

    # change our process name
    $0 = "simp($worker_name)";

    # setup signal handlers
    $SIG{'TERM'} = sub {

        $self->logger->info($self->worker_name. " Received SIG TERM." );
        $self->stop();
    };

    $SIG{'HUP'} = sub {

        $self->logger->info($self->worker_name. " Received SIG HUP." );
    };

    # connect to redis
    my $redis_host = $self->global_config->{'redis_host'};
    my $redis_port = $self->global_config->{'redis_port'};

    $self->logger->debug($self->worker_name." Connecting to Redis $redis_host:$redis_port." );

    my $redis;

    try {
        #--- try to connect twice per second for 30 seconds, 60 attempts every 500ms.
        $redis = Redis->new(
            server    => "$redis_host:$redis_port",
            reconnect => 60,
            every     => 500,
            read_timeout => 2,
            write_timeout => 3,
            );
    }
    catch {
        $self->logger->error($self->worker_name." Error connecting to Redis: $_" );
    };

    $self->_set_redis( $redis );

    $self->_set_need_restart(0);

    $self->logger->debug( $self->worker_name . ' Starting SNMP Poll loop.' );

    $self->_connect_to_snmp();

    $self->logger->debug($self->worker_name . ' var_hosts: "' . (join '", "', (keys %{$self->var_hosts})) . '"');

    $self->{'collector_timer'} = AnyEvent->timer( after => 10,
                                                  interval => $self->poll_interval,
                                                  cb => sub {
                                                      my $cycle_cv = AnyEvent->condvar;
                                                      $cycle_cv->begin(sub { undef $cycle_cv; $self->_write_heartbeat(); });
                                                      $self->_collect_data($cycle_cv);
                                                      $cycle_cv->end;
                                                      AnyEvent->now_update;
                                                  });


    #let the magic happen
    my $cv = AnyEvent->condvar;
    $self->_set_main_cv($cv);
    $cv->recv;
    $self->logger->error("Exiting");
}

=item stop

=back

=cut

sub stop{
    my $self = shift;
    $self->logger->error("Stop was called");
    $self->main_cv->send();
}

sub _poll_cb{
    my $self = shift;
    my %params = @_;

    my $cycle_cv  = $params{'cycle_cv'};
    my $session   = $params{'session'};
    my $host      = $params{'host'};
    my $req_time  = $params{'timestamp'};
    my $reqstr    = $params{'reqstr'};
    my $main_oid  = $params{'oid'};
    my $context_id = $params{'context_id'};
    my $redis     = $self->redis;
    my $data      = $session->var_bind_list();

    my $id        = $self->group_name;
    my $timestamp = $req_time;
    my $ip        = $host->{'ip'};


    if(defined($context_id)){
        delete $host->{'pending_replies'}->{$main_oid . "," . $context_id};
    }else{
        delete $host->{'pending_replies'}->{$main_oid};
    }

    if(!defined $data){
        my $error = $session->error();
        $self->logger->error("Host/Context ID: $host->{'node_name'} " . (defined($context_id) ? $context_id : '[no context ID]'));
        $self->logger->error("$id failed     $reqstr");
        $self->logger->error("Error: $error");

        $host->{'poll_status'} = 'TO';
        if (scalar(keys %{$host->{'pending_replies'}}) == 0){
            $self->_write_host_status($host, $timestamp);
            $cycle_cv->end;
        }
        return;
    }

    my @values;

    for my $oid (keys %$data){
        push(@values, "$oid," .  $data->{$oid});
    }

    my $expires = $timestamp + $self->retention;

    try {

        $redis->select(DB_MAIN);
        my $key = $host->{'node_name'} . "," . $self->worker_name . ",$timestamp";
        $redis->sadd($key, @values);

        if(scalar(keys %{$host->{'pending_replies'}}) == 0){
            #$self->logger->error("Received all responses!");
            $redis->expireat($key, $expires);

            #our poll_id to time lookup
            $redis->select(DB_POLLID_TO_KEY);
            $redis->set($host->{'node_name'} . "," . $self->group_name . "," . $host->{'poll_id'}, $key);
            $redis->set($ip . "," . $self->group_name . "," . $host->{'poll_id'}, $key);
            #and expire
            $redis->expireat($host->{'node_name'} . "," . $self->group_name . "," . $host->{'poll_id'}, $expires);
            $redis->expireat($ip . "," . $self->group_name . "," . $host->{'poll_id'},$expires);

            $redis->select(DB_MAIN);
            #$self->logger->error(Dumper($host->{'group'}{$self->group_name}));

            if($self->var_hosts()->{$host->{'node_name'}} && defined($host->{'host_variable'})){

                $self->logger->debug("Adding host variables for $host->{'node_name'}");

                my %add_values = %{$host->{'host_variable'}};
                foreach my $name (keys %add_values){
                    my $sanitized_name = $name;
                    $sanitized_name =~ s/,//g; # we don't allow commas in variable names
                    my $str = 'vars.' . $sanitized_name . "," . $add_values{$name};
                    $redis->select(DB_MAIN);
                    $redis->sadd($key, $str);
                }

                $redis->select(DB_OID_MAP);
                $redis->hset($host->{'node_name'}, "vars", $self->group_name . "," . $self->poll_interval);
                $redis->hset($ip, "vars", $self->group_name . "," . $self->poll_interval);
            }

            #and the current poll_id lookup
            $redis->select(DB_CURRENT_POLLID);
            $redis->set($host->{'node_name'} . "," . $self->group_name, $host->{'poll_id'} . "," . $timestamp);
            $redis->set($ip . "," . $self->group_name, $host->{'poll_id'} . "," . $timestamp);
            #and expire
            $redis->expireat($host->{'node_name'} . "," . $self->group_name, $expires);
            $redis->expireat($ip . "," . $self->group_name, $expires);

            $host->{'poll_id'}++;
        }


        $redis->select(DB_OID_MAP);
        $redis->hset($host->{'node_name'}, $main_oid, $self->group_name . "," . $self->poll_interval);
        $redis->hset($ip, $main_oid, $self->group_name . "," . $self->poll_interval);

        #change back to the primary db...
        $redis->select(DB_MAIN);
        #complete the transaction
    } catch {
        $redis->select(DB_MAIN);
        $self->logger->error($self->worker_name. " $id Error in hset for data: $_" );
    };

    if(scalar(keys %{$host->{'pending_replies'}}) == 0){
        $self->_write_host_status($host, $timestamp);
        $cycle_cv->end;
    }

    AnyEvent->now_update;
}

sub _connect_to_snmp{
    my $self = shift;
    my $hosts = $self->hosts;
    foreach my $host(@$hosts){
        # build the SNMP object for each host of interest
        my ($snmp,$error);
        if(!defined($host->{'snmp_version'}) || $host->{'snmp_version'} eq '2c'){

            ($snmp, $error) = Net::SNMP->session(
                -hostname         => $host->{'ip'},
                -community        => $host->{'community'},
                -version          => 'snmpv2c',
                -timeout          => $self->snmp_timeout,
                -maxmsgsize       => 65535,
                -translate        => [-octetstring => 0],
                -nonblocking      => 1,
            );

            if(!defined($snmp)){
                $self->logger->error("Error creating SNMP Session: " . $error);
            }

            $self->{'snmp'}{$host->{'node_name'}} = $snmp;

        }elsif($host->{'snmp_version'} eq '3'){
            if(scalar(@{$host->{'groups'}{$self->group_name}}) == 0){
                ($snmp, $error) = Net::SNMP->session(
                    -hostname         => $host->{'ip'},
                    -version          => '3',
                    -timeout          => $self->snmp_timeout,
                    -maxmsgsize       => 65535,
                    -translate        => [-octetstring => 0],
                    -username         => $host->{'username'},
                    -nonblocking      => 1,
                    );

                if(!defined($snmp)){
                    $self->logger->error("Error creating SNMP Session: " . $error);
                }

                $self->{'snmp'}{$host->{'node_name'}} = $snmp;
            }else{
                foreach my $ctxEngine (@{$host->{'groups'}{$self->group_name}}){
                    ($snmp, $error) = Net::SNMP->session(
                        -hostname         => $host->{'ip'},
                        -version          => '3',
                        -timeout          => $self->snmp_timeout,
                        -maxmsgsize       => 65535,
                        -translate        => [-octetstring => 0],
                        -username         => $host->{'username'},
                        -nonblocking      => 1,
                        );

                    if(!defined($snmp)){
                        $self->logger->error("Error creating SNMP Session: " . $error);
                    }

                    $self->{'snmp'}{$host->{'node_name'}}{$ctxEngine} = $snmp;
                }
            }
        }else{
            $self->logger->error("Invalid SNMP Version for SIMP");
        }

        my $host_poll_id;
        try{
            $self->redis->select(DB_CURRENT_POLLID);
            $host_poll_id = $self->redis->get($host->{'node_name'} . ",main_oid");
            $self->redis->select(DB_MAIN);

            if(!defined($host_poll_id)){
                $host_poll_id = 0;
            }

        }catch{
            $self->redis->select(DB_MAIN);
            $host_poll_id = 0;
            $self->logger->error("Error fetching the current poll cycle id from redis: $_");
        };

        $host->{'poll_id'} = $host_poll_id;
        $host->{'pending_replies'} = {};

        $self->logger->debug($self->worker_name . " assigned host " . $host->{'node_name'});
    }
}

sub _collect_data{
    my $self = shift;
    my $cycle_cv = shift;

    my $hosts         = $self->hosts;
    my $oids          = $self->oids;
    my $timestamp     = time;
    $self->logger->debug($self->worker_name. " start poll cycle" );

    for my $host (@$hosts){

        if(scalar(keys %{$host->{'pending_replies'}}) > 0){
            $self->logger->error("Unable to query device " . $host->{'ip'} . ":" . $host->{'node_name'} . " in poll cycle for group: " . $self->group_name);
            next;
        }

        $cycle_cv->begin;

        for my $oid (@$oids){
            if(!defined($self->{'snmp'}{$host->{'node_name'}})){
                $self->logger->error("No SNMP session defined for " . $host->{'node_name'});
                next;
            }
            my $reqstr = " $oid -> $host->{'ip'} ($host->{'node_name'})";
            $self->logger->debug($self->worker_name ." requesting ". $reqstr);
            #--- iterate through the the provided set of base OIDs to collect
            my $res;

            if($host->{'snmp_version'} eq '2c'){
                $host->{'pending_replies'}->{$oid} = 1;
                $res = $self->{'snmp'}{$host->{'node_name'}}->get_table(
                    -baseoid      => $oid,
                    -maxrepetitions => $self->max_reps,
                    -callback     => sub{
                        my $session = shift;
                        $self->_poll_cb( host => $host,
                                         timestamp => $timestamp,
                                         reqstr => $reqstr,
                                         oid => $oid,
                                         session => $session,
                                         cycle_cv => $cycle_cv);
                    }
                    );
            }else{
                #for each context engine specified for the group
                if(scalar(@{$host->{'groups'}{$self->group_name}}) == 0){
                    $host->{'pending_replies'}->{$oid} = 1;
                    $res = $self->{'snmp'}{$host->{'node_name'}}->get_table(
                        -baseoid      => $oid,
                        -maxrepetitions => $self->max_reps,
                        -callback     => sub{
                            my $session = shift;
                            $self->_poll_cb( host => $host,
                                             timestamp => $timestamp,
                                             reqstr => $reqstr,
                                             oid => $oid,
                                             session => $session,
                                             cycle_cv => $cycle_cv);
                        }
                        );

                }else{
                    foreach my $ctxEngine (@{$host->{'groups'}{$self->group_name}}){
                        $host->{'pending_replies'}->{$oid . "," . $ctxEngine} = 1;
                        $res = $self->{'snmp'}{$host->{'node_name'}}{$ctxEngine}->get_table(
                            -baseoid      => $oid,
                            -maxrepetitions => $self->max_reps,
                            -contextengineid => $ctxEngine,
                            -callback     => sub{
                                my $session = shift;
                                $self->_poll_cb( host => $host,
                                                 timestamp => $timestamp,
                                                 reqstr => $reqstr,
                                                 oid => $oid,
                                                 context_id => $ctxEngine,
                                                 session => $session,
                                                 cycle_cv => $cycle_cv);
                            }
                            );
                    }
                }
            }
        }
    }
}

sub _write_host_status {
    my $self = shift;
    my $host = shift;
    my $timestamp = shift;

    my $redis = $self->redis;
    my $host_group = $host->{'node_name'} . ',' . $self->group_name;

    my $monitoring_key = "simp-poller,host-status,$host_group";
    my $poll_status = $host->{'poll_status'};

    # "Happy families are all alike; every unhappy family is unhappy in its own way."
    #    --Leo Tolstoy, _Anna Karenina_
    $poll_status = 'S' if !defined($poll_status);

    try {
        $redis->select(DB_MONITORING);
        $redis->lpush($monitoring_key, "$timestamp,$poll_status");
        $redis->ltrim($monitoring_key, 0, N);
        $redis->expire($monitoring_key, max(2 * N * $self->poll_interval, 86400));
        $redis->select(DB_MAIN);
    } catch {
        $self->logger->error($self->worker_name . " unable to write monitoring status for ($host_group) to Redis");
        $redis->select(DB_MAIN);
    }

    $host->{'poll_status'} = undef;
}

sub _write_heartbeat {
    my $self = shift;

    my $redis = $self->redis;
    my $key = 'simp-poller,heartbeat,' . $self->worker_name;

    try {
        $redis->select(DB_MONITORING);
        $redis->set($key, time);
        $redis->expire($key, max(2 * N * $self->poll_interval, 86400));
        $redis->select(DB_MAIN);
    } catch {
        $self->logger->error($self->worker_name . ' unable to write heartbeat to Redis');
        $redis->select(DB_MAIN);
    }
}

1;
