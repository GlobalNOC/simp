Summary: A system for fetching data from simp and compiling the data into a composite 
Name: simp-comp
Version: 1.0.6
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-comp-%{version}.tar.gz

BuildRequires: perl
BuildRequires: perl(Test::Deep)
BuildRequires: perl(Test::More)
BuildRequires: perl(Test::Pod) >= 1.22
BuildRequires: rabbitmq-server

Requires: perl(AnyEvent)
Requires: perl(List::MoreUtils)
Requires: perl(Data::Munge)
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-GRNOC-RabbitMQ >= 1.1.1
Requires: perl-Moo
Requires: perl-Parallel-ForkManager
Requires: perl-Try-Tiny
Requires: perl-Type-Tiny

Provides: perl(GRNOC::Simp::CompData)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-comp-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/CompData
%{__install} -d -p %{buildroot}/etc/init.d
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/simp

%{__install} lib/GRNOC/Simp/CompData.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/CompData.pm
%{__install} lib/GRNOC/Simp/CompData/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/CompData/Worker.pm
%{__install} bin/simp-comp.pl %{buildroot}/usr/bin/simp-comp.pl
%{__install} conf/compDataConfig.xml %{buildroot}/etc/simp/compDataConfig.xml
%{__install} conf/logging.conf %{buildroot}/etc/simp/comp_logging.conf
%{__install} conf/simp_comp.init %{buildroot}/etc/init.d/simp-comp

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/CompData.pm
%{perl_vendorlib}/GRNOC/Simp/CompData/Worker.pm
%defattr(755,root,root,755)
/usr/bin/simp-comp.pl
/etc/init.d/simp-comp
%defattr(644,root,root,755)
%config(noreplace) /etc/simp/comp_logging.conf
%config(noreplace) /etc/simp/compDataConfig.xml

%doc


%changelog
* Fri Mar 10 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.2
  - Fixes for holes in graphs
  - improvements in the poller
  - creation of the purger
* Fri Jan 20 2017 Andrew Ragusa <aragusa@globalnoc.iu.edu> - 1.0.0
- Initial build.

