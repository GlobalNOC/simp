use strict;
use warnings;
use ExtUtils::MakeMaker;

use FindBin;

sub MY::postamble { <<"END"; }

test_jenkins :
	PERL5OPT=-MDevel::Cover=-db,$FindBin::Bin/cover_db make test PERL_TEST_HARNESS_DUMP_TAP=$FindBin::Bin/tap/

END

WriteMakefile(
    NAME                => 'perl-SIMP',
    AUTHOR              => 'GRNOC Systems Engineering <syseng@grnoc.iu.edu>',
    VERSION_FROM        => 'lib/GRNOC/Simp.pm',
    PL_FILES            => {},
    PREREQ_PM => {
    'GRNOC::RabbitMQ' => 0,
    'redis' => 0,
    'GRNOC::Config' => 0,
    'GRNOC::Log' => 0
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'perl-SIMP-*' },
    );

