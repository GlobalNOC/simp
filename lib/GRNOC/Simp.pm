#--------------------------------------------------------------------
#----- GRNOC Simp Library
#-----
#----- Copyright(C) 2016 The Trustees of Indiana University
#--------------------------------------------------------------------
#-----
#----- This module doesn't do much other than storing the version
#----- of Simp.  All the magic happens in the DataService and GWS
#----- libraries.
#--------------------------------------------------------------------

package GRNOC::Simp;

use strict;
use warnings;

our $VERSION = '1.0.3';

sub new {
    my $caller = shift;

    my $class = ref( $caller );
    $class = $caller if ( !$class );

    my $self = {
        @_
    };

    bless( $self, $class );

    return $self;
}

sub get_version {
    my $self = shift;

    return $VERSION;
}

1;
