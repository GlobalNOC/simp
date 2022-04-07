package GRNOC::Simp::TSDS::Pusher;

use strict;
use warnings;

# Messages pushed with invalid chars have their contents dumped to the error log
# For this reason, we set Dumper to use a more compact output format
use Data::Dumper;
$Data::Dumper::Terse = 1;
$Data::Dumper::Indent = 0;

use Moo;
use JSON::XS qw(encode_json);

use GRNOC::Simp::TSDS;
use GRNOC::WebService::Client;

use constant {
    MAX_TSDS_MESSAGES  => 50,
    SERVICE_CACHE_FILE => "/etc/grnoc/name-service-cacher/name-service.xml",
};

=head2 public attributes

=over 12

=item logger

=item worker_name

=item tsds_config

=item tsds_svc

=back

=cut

has logger => (
    is       => 'rwp',
    required => 1
);

has worker_name => (
    is       => 'ro',
    required => 1
);

has tsds_config => (
    is       => 'rwp',
    required => 1
);

has tsds_svc => (is => 'rwp');


=head2 BUILD

=cut

sub BUILD
{
    my ($self) = @_;

    # Set up our TSDS webservice object when construcuted
    $self->_set_tsds_svc(
        GRNOC::WebService::Client->new(
            url => $self->tsds_config->{'url'},

            # urn => $self->tsds_config->{'urn'},
            uid    => $self->tsds_config->{'user'},
            passwd => $self->tsds_config->{'password'},
            realm  => $self->tsds_config->{'realm'},

            # service_cache_file => SERVICE_CACHE_FILE,
            usePost => 1
        )
    );
}

=head2 push

=cut

sub push {
    my ($self, $msg_list) = @_;

    # Return early if there are no messages to send
    if (scalar(@$msg_list) < 1) {
        $self->logger->info(sprintf("[%s] Nothing to push to TSDS", $self->worker_name));
        return;
    };

    # Add the block of messages to TSDS using push.cgi
    my $res = $self->tsds_svc->add_data(data => encode_json($msg_list));

    # Check the response from TSDS
    if (!defined($res) || $res->{'error'}) {

        my $error = sprintf("[%s] Error pushing to TSDS: %s", $self->worker_name, GRNOC::Simp::TSDS::error_message($res));

        # Check whether the issue was sending a message that has invalid characters
        if ($res && $res->{error_text} && $res->{error_text} =~ m/only accepts printable characters/g) {

            # Track individual bad messages to dump in the error log
            my @bad;

            # Try sending each message individually in-case only one had character issues
            for my $msg (@$msg_list) {
                $res = $self->tsds_svc->add_data(data => encode_json([$msg]));
                if (!defined($res) || $res->{error}) {
                    push(@bad, $msg);
                }
            }

            # Update the error message
            $error .= " - %s out of %s total messages contained invalid characters: %s";
            $error = sprintf($error, scalar(@bad), scalar(@$msg_list), Dumper(\@bad));
        } else {
            # Missing response should be reported as error i.e. connection refused by RabbitMQ
            $res = {'error' => $error};
        }
        $self->logger->error($error);
    }
    # Return hash ref for worker's status log
    return $res;
}

1;
