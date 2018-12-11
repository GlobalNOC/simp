Summary: A small system for fetching SNMP data from redis and returning it via RabbitMQ
Name: simp-data
Version: 1.0.11
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
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/simp

%{__install} lib/GRNOC/Simp/Data.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{__install} lib/GRNOC/Simp/Data/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{__install} bin/simp-data.pl %{buildroot}/usr/bin/simp-data.pl
%{__install} conf/simpDataConfig.xml %{buildroot}/etc/simp/
%{__install} conf/logging.conf %{buildroot}/etc/simp/data_logging.conf
%{__install} conf/simp-data.systemd %{buildroot}/etc/systemd/system/simp-data.service

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%defattr(755,root,root,755)
/usr/bin/simp-data.pl
/etc/systemd/system/simp-data.service
%defattr(644,root,root,755)
%config(noreplace) /etc/simp/simpDataConfig.xml
%config(noreplace) /etc/simp/data_logging.conf
%doc


%changelog
* Fri Mar 10 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.2
  - Fixes for holes in graphs
  - improvements in the poller
  - creation of the purger
* Fri Jan 20 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.0
- Initial build.

