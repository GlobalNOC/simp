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
use Syntax::Keyword::Try;

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

    # Initialize a response and error.
    my $res;
    my $error;

    # Return the default success response if there are no messages.
    if (scalar(@$msg_list) < 1) {
        $self->logger->info(sprintf("[%s] Nothing to push to TSDS", $self->worker_name));
        return {'error' => 0, 'error_text' => ''};
    };

    # Try pushing the data to TSDS.
    try {
        $res = $self->tsds_svc->add_data(data => encode_json($msg_list));
    }
    catch ($e) {
        $error = sprintf("[%s] Error: Exception while pushing data to TSDS: %s", $self->worker_name, $e);
        $self->logger->error($error);

        # Return a response hashref with the error.
        return {'error' => 1, 'error_text' => $error};
    };

    # Handle an undefined response.
    if (!defined($res)) {
        $error = sprintf("[%s] Error: No response from TSDS push.cgi", $self->worker_name);

        # Return a response hashref with the error.
        return {'error' => 1, 'error_text' => $error};
    }
    # Handle normal response errors.
    elsif (ref($res) eq 'HASH' && exists($res->{'error'}) && $res->{'error'}) {
        $error = sprintf("[%s] Error: Response from TSDS push.cgi had errors", $self->worker_name);

        # Some response errors will include a message
        if (exists($res->{'error_text'}) && defined($res->{'error_text'})) {
            $error = sprintf("%s: %s", $error, $res->{'error_text'});

            # Check whether the issue was sending a message that has invalid characters.
            # Try re-sending the data one message at a time if so.
            # Can't recurse push() on individual messages, a bad message will always fail.
            if ($res->{'error_text'} =~ m/only accepts printable characters/g) {
                $error = sprintf("[%s] Error: Message with bad characters encountered, pushing messages individually", $self->worker_name);

                my @bad_messages;

                for my $msg (@$msg_list) {
                    try {
                        $res = $self->tsds_svc->add_data(data => encode_json([$msg]));
                    }
                    catch ($e) {
                        $self->logger->error(sprintf("[%s] Error: Exception while retrying individual message push to TSDS: %s", $self->worker_name, $e));
                    };

                    # A message is bad unless it gets a normal response without errors.
                    unless (defined($res) && ref($res) eq 'HASH' && !$res->{'error'}) {
                        $self->logger->error(sprintf("[%s] Error: Found a bad message after retry: %s", $self->worker_name, Dumper($msg)));
                        push(@bad_messages, $msg);
                    }
                }

                # Update the error message
                $error .= ": %s out of %s total messages contained invalid characters";
                $error = sprintf($error, scalar(@bad_messages), scalar(@$msg_list));
            }
        }
        $self->logger->error($error);
        return {'error' => 1, 'error_text' => $error};
    }
    # Handle abnormal response formats
    elsif (ref($res) ne 'HASH') {
        $error = sprintf("[%s] Error: Abnormal response of type %s from push.cgi", self->worker_name, ref($res));
        $self->logger->error($error);
        return {'error' => 1, 'error_text' => $error};
    }

    # Force the error and error_text keys if they don't exist for some reason
    $res->{'error'} = 0 unless (exists($res->{'error'}));
    $res->{'error_text'} = '' unless(exists($res->{'error_text'}));

    return $res;
}

1;
