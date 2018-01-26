use strict;
use warnings;
use ExtUtils::MakeMaker;

use FindBin;

sub MY::postamble { << 'END'; }
rpm:	dist
	rpmbuild -ta simp-$(VERSION).tar.gz

END

WriteMakefile(
    NAME                => 'simp',
    AUTHOR              => 'GRNOC Systems Engineering <syseng@grnoc.iu.edu>',
    VERSION_FROM        => 'lib/GRNOC/Simp.pm',
    PL_FILES            => {},
    PREREQ_PM => {
    'GRNOC::RabbitMQ' => 0,
    'Redis' => 0,
    'GRNOC::Config' => 0,
    'GRNOC::Log' => 0
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'simp-*' },
    );

sub MY::test
{
    q(
TEST_VERBOSE=1
test : pure_all
	$(FULLPERL) t/TEST $(TEST_VERBOSE)
test_jenkins : pure_all
	$(FULLPERL) t/TEST $(TEST_VERBOSE) --formatter=TAP::Formatter::Console
    );
}
