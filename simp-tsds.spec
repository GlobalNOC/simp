Summary: SIMP TSDS Collector
Name: simp-tsds
Version: 1.2.2
Release: 1%{dist}
License: APL 2.0
Group: Network
URL: http://globalnoc.iu.edu
Source0: %{name}-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildArch:noarch

BuildRequires: perl
Requires: perl(AnyEvent)
Requires: perl(AnyEvent::Subprocess)
Requires: perl(Data::Dumper)
Requires: perl(Getopt::Long)
Requires: perl(JSON::XS)
Requires: perl(List::MoreUtils)
Requires: perl(Moo)
Requires: perl(MooseX::Clone)
Requires: perl(Parallel::ForkManager)
Requires: perl(POSIX)
Requires: perl(Proc::Daemon)
Requires: perl(Types::Standard)
Requires: perl(GRNOC::Config)
Requires: perl(GRNOC::WebService::Client)
Requires: perl(GRNOC::RabbitMQ::Client)
Requires: perl(GRNOC::Log)

%define execdir /usr/bin
%define configdir /etc/simp/tsds
%define sysconfdir /etc/sysconfig

%description
This program pulls SNMP-derived data from Simp and publishes it to TSDS.

%pre
/usr/bin/getent group simp > /dev/null || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp > /dev/null || /usr/sbin/useradd -r -s /bin/nologin -g simp simp

%prep
%setup -q

%build

%install
rm -rf $RPM_BUILD_ROOT
%{__mkdir} -p -m 0775 $RPM_BUILD_ROOT%{execdir}
%{__mkdir} -p -m 0775 $RPM_BUILD_ROOT%{configdir}/collections.d
%{__mkdir} -p -m 0775 $RPM_BUILD_ROOT%{configdir}/validation.d
%{__mkdir} -p -m 0775 $RPM_BUILD_ROOT%{sysconfdir}
%{__mkdir} -p -m 0775 $RPM_BUILD_ROOT%{perl_vendorlib}/GRNOC/Simp/TSDS
%{__install} bin/simp-tsds.pl $RPM_BUILD_ROOT/%{execdir}/
%{__install} conf/tsds/config.xml $RPM_BUILD_ROOT/%{configdir}/config.xml
%{__install} conf/tsds/config.xsd $RPM_BUILD_ROOT/%{configdir}/validation.d/config.xsd
%{__install} conf/tsds/collection.xml.example $RPM_BUILD_ROOT/%{configdir}/collections.d/collection.xml.example
%{__install} conf/tsds/collection.xsd $RPM_BUILD_ROOT/%{configdir}/validation.d/collection.xsd
%{__install} conf/logging.conf $RPM_BUILD_ROOT/%{configdir}/logging.conf

%if 0%{?rhel} >= 7
%{__install} -d -p %{buildroot}/etc/systemd/system/
%{__install} conf/tsds/simp-tsds.systemd $RPM_BUILD_ROOT/etc/systemd/system/simp-tsds.service
%else
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} conf/tsds/simp-tsds.service %{buildroot}/etc/init.d/simp-tsds
%{__install} conf/sysconfig $RPM_BUILD_ROOT/%{sysconfdir}/simp-tsds
%endif
%{__install} lib/GRNOC/Simp/TSDS.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/
%{__install} lib/GRNOC/Simp/TSDS/Master.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/TSDS/
%{__install} lib/GRNOC/Simp/TSDS/Worker.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/TSDS/
%{__install} lib/GRNOC/Simp/TSDS/Pusher.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/TSDS/
# clean up buildroot
find %{buildroot} -name .packlist -exec %{__rm} {} \;

%{_fixperms} $RPM_BUILD_ROOT/*

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(644,root,root,755)
%attr(755,root,root) %{execdir}/simp-tsds.pl
%if 0%{?rhel} >= 7
%attr(644,root,root) /etc/systemd/system/simp-tsds.service
%else
%attr(755,root,root) /etc/init.d/simp-tsds
%config(noreplace) %{sysconfdir}/simp-tsds
%endif
%{perl_vendorlib}/GRNOC/Simp/TSDS.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Master.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Pusher.pm
%config(noreplace) %{configdir}/config.xml
%config(noreplace) %{configdir}/logging.conf
%config(noreplace) %{configdir}/collections.d/*

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
* Tue May 23 2017 AJ Ragusa <aragusa@globalnoc.iu.edu> - SIMP Collector
* Fri Feb 24 2017 CJ Kloote <ckloote@globalnoc.iu.edu> - OESS VLAN Collector
- Initial build.
