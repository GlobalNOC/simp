Summary: A small system for fetching SNMP data from redis and returning it via RabbitMQ
Name: simp-data
Version: 1.3.0
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-data-%{version}.tar.gz

BuildRequires: perl
BuildRequires: perl(Test::Deep)
BuildRequires: perl(Test::More)
BuildRequires: perl(Test::Pod) >= 1.22
BuildRequires: rabbitmq-server

Requires: redis
Requires: perl(AnyEvent)
Requires: perl(List::MoreUtils)
Requires: perl(Data::Munge)
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-GRNOC-RabbitMQ >= 1.1.1
Requires: perl-Moo
Requires: perl-Parallel-ForkManager
Requires: perl(POSIX)
Requires: perl-Redis >= 1.991
Requires: perl-Try-Tiny
Requires: perl-Type-Tiny
Requires: perl(Class::Accessor::Fast)

Provides: perl(GRNOC::Simp::Data)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-data-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data
%{__install} -d -p %{buildroot}/etc/systemd/system/
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/simp
%{__install} -d -p %{buildroot}/etc/simp/data
%{__install} -d -p %{buildroot}/etc/simp/data/validation.d

%{__install} lib/GRNOC/Simp/Data.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{__install} lib/GRNOC/Simp/Data/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{__install} bin/simp-data.pl %{buildroot}/usr/bin/simp-data.pl
%{__install} conf/data/config.xml %{buildroot}/etc/simp/data/config.xml
%{__install} conf/data/config.xsd %{buildroot}/etc/simp/data/validation.d/config.xsd
%{__install} conf/logging.conf %{buildroot}/etc/simp/data/logging.conf

%if 0%{?rhel} >= 7
%{__install} conf/data/simp-data.systemd %{buildroot}/etc/systemd/system/simp-data.service
%else
%{__install} conf/data/simp-data.service %{buildroot}/etc/init.d/simp-data
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm

%defattr(755,root,root,755)
/usr/bin/simp-data.pl

%defattr(644,root,root,644)
/etc/simp/data/validation.d/config.xsd

%if 0%{?rhel} >= 7
/etc/systemd/system/simp-data.service
%else
/etc/init.d/simp-data
%endif

%defattr(644,root,root,755)
%config(noreplace) /etc/simp/data/config.xml
%config(noreplace) /etc/simp/data/logging.conf

%doc


%changelog
* Mon May 06 2019 Vincent Orlowski <vincentorlowski@gmail.com> - 1.1.0
  - Poller now writes status files for each polling group per host for simp-poller-monitoring
  - Poller now has various error checks for monitoring
  - Comp now has the ability to scan a static OID
  - Comp now has the ability to use a scan's parameters within N other scans
  - Comp scans dependent on other scans will now perserve dependencies
  - Comp now has a refactored data structure for results
  - Comp has had various optimizations added
  - Comp now outputs an array of data objects instead of a hash
  - TSDS has been adjusted to use new output from Comp
  - Simp now has packaging and installation support for EL6
  - Simp now has init.d scripts for simp-poller, simp-data, simp-comp, and simp-tsds to support EL6 hosts
* Fri Mar 10 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.2
  - Fixes for holes in graphs
  - improvements in the poller
  - creation of the purger
* Fri Jan 20 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.0
- Initial build.

