Summary: SIMP TSDS Collector
Name: simp-tsds
Version: 1.8.3
Release: 1%{dist}
License: APL 2.0
Group: Network
URL: http://globalnoc.iu.edu
Source0: %{name}-%{version}.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildArch:noarch

# This prevents automatic dependency resolution from failing in external imports.
# Without it, the created RPMs may not install properly
AutoReqProv: no

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
Requires: perl(GRNOC::RabbitMQ) >= 1.2.2
Requires: perl(GRNOC::Log)

%define execdir /usr/bin
%define configdir /etc/simp/tsds
%define sysconfdir /etc/sysconfig

%description
This program pulls SNMP-derived data from Simp and publishes it to TSDS.

%pre
/usr/bin/getent group simp > /dev/null || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp > /dev/null || /usr/sbin/useradd -r -s /bin/nologin -g simp simp

%post
systemctl daemon-reload

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
%{__install} bin/simp-tsds.pl $RPM_BUILD_ROOT/%{execdir}
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

%defattr(644,root,root,644)
/etc/simp/tsds/validation.d/config.xsd
/etc/simp/tsds/validation.d/collection.xsd
