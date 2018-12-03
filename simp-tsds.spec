Summary: SIMP TSDS Collector
Name: simp-tsds
Version: 1.0.9
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
%define configdir /etc/simp
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
%__mkdir -p -m 0775 $RPM_BUILD_ROOT%{execdir}
%__mkdir -p -m 0775 $RPM_BUILD_ROOT%{configdir}
%__mkdir -p -m 0775 $RPM_BUILD_ROOT%{sysconfdir}
%__mkdir -p -m 0775 $RPM_BUILD_ROOT%{perl_vendorlib}/GRNOC/Simp/TSDS
%__install bin/simp-tsds.pl $RPM_BUILD_ROOT/%{execdir}/
%__install conf/simp-tsds.xml $RPM_BUILD_ROOT/%{configdir}/simp-tsds.xml
%__install conf/logging.conf $RPM_BUILD_ROOT/%{configdir}/simp_tsds_logging.conf
%if 0%{?rhel} == 7
%__install -d -p %{buildroot}/etc/systemd/system/
%__install conf/simp-tsds.systemd $RPM_BUILD_ROOT/etc/systemd/system/simp-tsds.service
%else
%__install conf/sysconfig $RPM_BUILD_ROOT/%{sysconfdir}/simp-tsds
%endif
%__install lib/GRNOC/Simp/TSDS.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/
%__install lib/GRNOC/Simp/TSDS/Master.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/TSDS/
%__install lib/GRNOC/Simp/TSDS/Worker.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/TSDS/
%__install lib/GRNOC/Simp/TSDS/Pusher.pm $RPM_BUILD_ROOT/%{perl_vendorlib}/GRNOC/Simp/TSDS/
# clean up buildroot
find %{buildroot} -name .packlist -exec %{__rm} {} \;

%{_fixperms} $RPM_BUILD_ROOT/*

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(644,root,root,755)
%attr(755,root,root) %{execdir}/simp-tsds.pl
%if 0%{?rhel} == 7
%attr(644,root,root) /etc/systemd/system/simp-tsds.service
%else
%config(noreplace) %{sysconfdir}/simp-tsds
%endif
%{perl_vendorlib}/GRNOC/Simp/TSDS.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Master.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Pusher.pm
%config(noreplace) %{configdir}/simp-tsds.xml
%config(noreplace) %{configdir}/simp_tsds_logging.conf

%changelog
* Tue May 23 2017 AJ Ragusa <aragusa@globalnoc.iu.edu> - SIMP Collector
* Fri Feb 24 2017 CJ Kloote <ckloote@globalnoc.iu.edu> - OESS VLAN Collector
- Initial build.
