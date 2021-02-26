package GRNOC::Simp::TSDS;

use strict;
use warnings;

our $VERSION = '1.6.1';

=head1 NAME

GRNOC::Simp::TSDS

=head2 new

=cut

sub new
{
    my $caller = shift;

    my $class = ref($caller);
    $class = $caller if (!$class);

    my $self = {@_};

    bless($self, $class);

    return $self;
}

=head2 get_version

=cut

sub get_version
{
    my $self = shift;
    return $VERSION;
}

=head2 error_message

=cut

sub error_message
{
    my $res = shift;

    if (!defined($res))
    {
        my $msg = ' [no response object]';
        $msg .= " \$!='$!'" if defined($!) && ($! ne '');
        return $msg;
    }

    my $msg = '';
    $msg .= " error=\"$res->{'error'}\"" if defined($res->{'error'});
    $msg .= " error_text=\"$res->{'error_text'}\""
      if defined($res->{'error_text'});
    $msg .= " \$!=\"$!\"" if defined($!) && ($! ne '');
    $msg .= " \$@=\"$@\"" if defined($@) && ($@ ne '');
    return $msg;
}

1;
