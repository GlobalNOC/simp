Summary: A small system for gathering large amounts of SNMP data and pushing them into redis
Name: simp-poller
Version: 1.10.0
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-poller-%{version}.tar.gz

# This prevents automatic dependency resolution from failing in external imports.
# Without it, the created RPMs may not install properly
AutoReqProv: no

BuildRequires: perl
BuildRequires: perl(Test::Deep)
BuildRequires: perl(Test::More)
BuildRequires: perl(Test::Pod) >= 1.22

Requires: redis
Requires: perl(AnyEvent)
Requires: perl(AnyEvent::SNMP)
Requires: perl(List::MoreUtils)
Requires: perl(Data::Munge)
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-Moo
Requires: perl-Net-SNMP
Requires: perl-Net-SNMP-XS
Requires: perl-Parallel-ForkManager
Requires: perl(POSIX)
Requires: perl-Redis-Fast >= 0.28
Requires: perl-Syntax-Keyword-Try
Requires: perl-Type-Tiny
Requires: perl-Crypt-Rijndael

Provides: perl(GRNOC::Simp::Poller)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-poller-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%post
systemctl daemon-reload

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller
%{__install} -d -p %{buildroot}/etc/simp/poller/
%{__install} -d -p %{buildroot}/etc/simp/poller/hosts.d/
%{__install} -d -p %{buildroot}/etc/simp/poller/groups.d/
%{__install} -d -p %{buildroot}/etc/simp/poller/validation.d/
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/systemd/system/
%{__install} -d -p %{buildroot}/var/lib/simp/poller/

%{__install} lib/GRNOC/Simp/Poller.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{__install} lib/GRNOC/Simp/Poller/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
%{__install} bin/simp-poller.pl %{buildroot}/usr/bin/simp-poller.pl
%{__install} conf/poller/config.xml %{buildroot}/etc/simp/poller/config.xml
%{__install} conf/poller/config.xsd %{buildroot}/etc/simp/poller/validation.d/config.xsd
%{__install} conf/logging.conf %{buildroot}/etc/simp/poller/logging.conf
%{__install} conf/poller/hosts.xml.example %{buildroot}/etc/simp/poller/hosts.d/hosts.xml.example
%{__install} conf/poller/group.xml.example %{buildroot}/etc/simp/poller/groups.d/group.xml.example
%{__install} conf/poller/hosts.xsd %{buildroot}/etc/simp/poller/validation.d/hosts.xsd
%{__install} conf/poller/group.xsd %{buildroot}/etc/simp/poller/validation.d/group.xsd

%if 0%{?rhel} >= 7
%{__install} conf/poller/simp-poller.systemd %{buildroot}/etc/systemd/system/simp-poller.service
%else
%{__install} conf/poller/simp-poller.service %{buildroot}/etc/init.d/simp-poller
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm

%defattr(644,root,root,644)
/etc/simp/poller/validation.d/config.xsd
/etc/simp/poller/validation.d/hosts.xsd
/etc/simp/poller/validation.d/group.xsd

%defattr(755,root,root,755)
/usr/bin/simp-poller.pl

%if 0%{?rhel} >= 7
/etc/systemd/system/simp-poller.service
%else
/etc/init.d/simp-poller
%endif

%defattr(644,root,root,755)
%config(noreplace) /etc/simp/poller/config.xml
%config(noreplace) /etc/simp/poller/hosts.d/*
%config(noreplace) /etc/simp/poller/groups.d/*
%config(noreplace) /etc/simp/poller/logging.conf

%defattr(755,simp,simp,755)
%dir /var/lib/simp/
%dir /var/lib/simp/poller/

%doc
