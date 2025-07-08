package GRNOC::Simp::Exporter;

use strict;
use warnings;
use Data::Dumper qw(Dumper);
use lib '/opt/grnoc/venv/simp/lib/perl5';

use Moo 2.003000;
use Types::Standard 1.004002 qw( Str Bool );
use Try::Tiny;
use GRNOC::Config;
use GRNOC::Log;
use JSON::XS;
use GRNOC::RabbitMQ::Method;
use GRNOC::RabbitMQ::Dispatcher;
use Net::AMQP::RabbitMQ;


use constant RECONNECT_TIMEOUT => 5;


our $VERSION = '1.11.3';



# Required Attributes
has config_file => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has logger => (
    is       => 'ro',
    isa      => Str,
    required => 1
);
has validation_file => (
    is       => 'ro',
    isa      => Str,
    required => 1
);

has run_user => (
    is       => 'ro',
    required => 0
);
has run_group => (
    is       => 'ro',
    required => 0
);

has rabbit => ( is => 'rwp' );
has json => ( is => 'rwp' );

=head2 private attributes
=over 12

=item config
=item logger
=item children

=back
=cut

has config => (is => 'rwp');
has logger => (is => 'rwp');


=head2 BUILD
    Creates the main Poller Moo object and process
=cut
sub BUILD {
    my ($self) = @_;

    # Create and store logger object
    $self->_set_logger($self->logger);

    # Create the config object
    my $config = GRNOC::Config->new(
        config_file => $self->config_file,
        force_array => 0
    );

    # Validate the config, exiting if there are errors
    my $validation_code = $config->validate($self->validation_file);

    if ($validation_code == 1) {
        $self->logger->debug("Successfully validated config file");
    }
    else {
        if ($validation_code == 0) {
            $self->logger->error("ERROR: Failed to validate $self->config_file!\n" . $config->{error}->{backtrace});
        }
        else {
            $self->logger->error("ERROR: XML schema in $self->validation_file is invalid!\n" . $config->{error}->{backtrace});
        }
        exit(1);
    }



    # Store the config object once it's been validated
    $self->_set_config($config->get('/config'));

    # create JSON object
    my $json = JSON::XS->new();
    $self->_set_json( $json );

    # connect to rabbit queues                                                                                                                                                                                                                                                      
    $self->_rabbit_connect();

    return $self;
}


sub _rabbit_connect {
    my ( $self ) = @_;
    my $rbmq_conf = $self->config->{'rabbitmq'};
   
    my $rabbit_host = $rbmq_conf->{'ip'};
    my $rabbit_port = $rbmq_conf->{'port'};
    my $rabbit_error_queue = 'Simp.Error';
    my $max_retries = 3;

    while ( 1 ) {

        $self->logger->info( "Connecting to RabbitMQ $rabbit_host:$rabbit_port." );

        my $connected = 0;

        try {

            my $rabbit = Net::AMQP::RabbitMQ->new();

            $rabbit->connect( $rabbit_host, {'port' => $rabbit_port} );

	        
            # open channel to the error queue we'll send to
            $rabbit->channel_open( 1 );
            $rabbit->queue_declare( 1, $rabbit_error_queue, {'auto_delete' => 0} );

            $self->_set_rabbit( $rabbit );

            $connected = 1;
        }
        catch {
            my $error = $_;
            warn("unable to connect to rabbitmq");
            warn($error);
            $self->logger->error( "Error connecting to RabbitMQ: $error" );
        };

        last if $connected || $max_retries-- == 0;
        
        $self->logger->info( "Reconnecting after " . RECONNECT_TIMEOUT . " seconds..." );
        sleep( RECONNECT_TIMEOUT );
    }
}

=head2 export
    Sends output to configured rabbit host
=cut
sub export {
    my ($self, $simp_part, $error_level, $error_type, $error_message) = @_;

    my %message = (
        simp_part     => $simp_part,
        error_type    => $error_type,
	    error_level   => $error_level,
        error_message => $error_message
    );

    my @messages = (\%message);  # wrap in arrayref for encoding

    try {
        $self->rabbit->publish(
            1,
            'Simp.Error',
            $self->json->encode(\@messages),
            { 'exchange' => '' }
        );
        
    }
    catch {
        my $error = $_;
        warn("error on pushing to rabbitmq" . $error);
        $self->logger->error("Failed to publish message to RabbitMQ: $error");
    };
}

1;
