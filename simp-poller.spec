Summary: A small system for gathering large amounts of SNMP data and pushing them into redis
Name: simp-poller
Version: 1.0.9
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-poller-%{version}.tar.gz

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
Requires: perl-Parallel-ForkManager
Requires: perl(POSIX)
Requires: perl-Redis >= 1.991
Requires: perl-Try-Tiny
Requires: perl-Type-Tiny

Provides: perl(GRNOC::Simp::Poller)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-poller-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller
%{__install} -d -p %{buildroot}/etc/simp/hosts.d
%{__install} -d -p %{buildroot}/etc/init.d
%{__install} -d -p %{buildroot}/usr/bin/

%{__install} lib/GRNOC/Simp/Poller.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{__install} lib/GRNOC/Simp/Poller/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
%{__install} bin/simp-poller.pl %{buildroot}/usr/bin/simp-poller.pl
%{__install} conf/config.xml %{buildroot}/etc/simp/
%{__install} conf/logging.conf %{buildroot}/etc/simp/poller_logging.conf
%{__install} conf/hosts.d/*.xml %{buildroot}/etc/simp/hosts.d/
%{__install} conf/simp-poller.init %{buildroot}/etc/init.d/simp-poller

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
%defattr(755,root,root,755)
/usr/bin/simp-poller.pl
/etc/init.d/simp-poller
%defattr(644,root,root,755)
%config(noreplace) /etc/simp/config.xml
%config(noreplace) /etc/simp/hosts.d/hosts.xml
%config(noreplace) /etc/simp/poller_logging.conf

%doc


%changelog
* Fri Mar 10 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.2
  - Fixes for holes in graphs
  - improvements in the poller
  - creation of the purger
* Fri Jan 20 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.0
- Initial build.

