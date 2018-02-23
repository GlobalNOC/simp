package GRNOC::Simp::TSDS::Creator;

use strict;
use warnings;

use AnyEvent;
use AnyEvent::Fork;
use Data::Dumper;
use Log::Log4perl;
use Proc::Daemon;

use GRNOC::Config;
use GRNOC::RabbitMQ::Client;

use constant MAX_PROCESSES => 3;


sub new {
    my $that  = shift;
    my $class = ref($that) || $that;

    my %args = (
        config => undef,
        pid    => undef,
        daemon => 0,
        user   => undef,
        group  => undef,
        @_
    );

    my $self = \%args;
    bless $self, $class;

    Log::Log4perl->init('/etc/simp/simp_tsds_logging.conf');
    $self->{'log'} = Log::Log4perl->get_logger('GRNOC.Simp.TSDS.Creator');

    my $conf = GRNOC::Config->new(
        config_file => $self->{'config'},
        force_array => 1
    );

    $self->{'simp'} = $conf->get('/config/simp')->[0];
    $self->{'exit'} = AnyEvent->condvar;

    $SIG{INT}  = sub { $self->signal_handler(); };
    $SIG{TERM} = sub { $self->signal_handler(); };
    $SIG{HUP}  = sub { $self->signal_handler(); };

    $self->configure_user_group();

    return $self;
}

sub signal_handler {
    my $self = shift;

    $self->{'log'}->warn("Caught sigint or sigterm. Sending stop to children.");
    $self->{'exit'}->send;
}

sub configure_user_group {
    my $self = shift;

    if (defined $self->{'group'}) {
	my $gid = getpwnam($self->{'group'});
	die "Unable to get GID '$self->{'group'}'\n" if !defined($gid);
	$! = 0;
	setgid($gid);
	die "Unable to set GID to '$self->{'group'}' ($gid): $!\n" if $! != 0;
    }

    if (defined $self->{'user'}) {
	my $uid = getpwnam($self->{'user'});
	die "Unable to get UID '$self->{'user'}'\n" if !defined($uid);
	$! = 0;
	setuid($uid);
	die "Unable to set UID to '$self->{'user'}' ($uid): $!\n" if $! != 0;
    }
}

sub start {
    my $self = shift;

    # If $self->{'daemon'} is set then fork off and exit the main
    # process.
    if ($self->{'daemon'}) {
	$self->{'log'}->info("Daemonizing collector!");

	my $daemon = Proc::Daemon->new(pid_file => $self->{'pid'});
	my $pid = $daemon->Init();

	if ($pid) {
            # Who knows if I need this or not?
	    # sleep 1;
	    die 'Spawning child process failed' if !$daemon->Status();
	    exit(0);
	}
    }


    for (my $i = 0; $i < MAX_PROCESSES; $i++) {
        $self->{'log'}->info("Starting worker $i.");
        AnyEvent::Fork->new->require("GRNOC::Simp::TSDS::Creation")->send_arg($i)->run("GRNOC::Simp::TSDS::Creation::run", my $cv = AnyEvent->condvar);
        $cv->recv;
    }

    # # Block until signal is received. Finish up by sending stop to
    # # each child created above. Calling ctrl-c on the command line
    # # sends SIGINT to the entire process group so calls to stop will
    # # timeout, but children are still killed.
    $self->{'exit'}->recv;
    $self->{'log'}->info("Shutting down children.");

    my $creation = GRNOC::RabbitMQ::Client->new(
        host => $self->{'simp'}->{'host'},
        port => $self->{'simp'}->{'port'},
        user => $self->{'simp'}->{'user'},
        pass => $self->{'simp'}->{'password'},
        exchange => 'SNAPP',
        topic    => 'SNAPP',
        timeout  => 3
    );

    my $complete = AnyEvent->condvar;
    $self->{'exit'}->begin(sub {
        $complete->send;
    });

    for (my $i = 0; $i < MAX_PROCESSES; $i++) {
        $self->{'exit'}->begin;
        $creation->{'topic'} = "SNAPP.$i";
        $creation->stop(
            async_callback => sub {
                my $resp = shift;
                if (defined $resp->{'error'}) {
                    $self->{'log'}->error($resp->{'error'});
                } else {
                    $self->{'log'}->info("$resp->{'results'}");
                }

                $self->{'exit'}->end;
            }
        );
    }

    $self->{'exit'}->end;
    $complete->recv;
}

1;
