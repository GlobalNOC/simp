Summary: A small system for gathering large amounts of SNMP data and providing a RabbitMQ mechanism to access them 
Name: grnoc-simp
Version: 1.0.0
Release: 2%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-%{version}.tar.gz
BuildRequires: perl
Requires: perl-GRNOC-RabbitMQ
Requires: perl-Moo
Requires: redis
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-Redis >= 1.991
Requires: perl-Parallel-ForkManager
Requires: perl-Try-Tiny
Requires: perl-Net-SNMP
Provides: perl(GRNOC::Simp::Data)
Provides: perl(GRNOC::Simp::CompData)
Provides: perl(GRNOC::Simp::Poller)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-%{version}

%build
%{__perl} Makefile.PL PREFIX="%{buildroot}%{_prefix}" INSTALLDIRS="vendor"
make

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/CompData
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller
%{__install} -d -p %{buildroot}/etc/grnoc/simp
%{__install} -d -p %{buildroot}/etc/init.d

%{__install} -d -p %{buildroot}/usr/bin/

%{__install} lib/GRNOC/Simp.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp.pm
%{__install} lib/GRNOC/Simp/Data.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{__install} lib/GRNOC/Simp/CompData.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/CompData.pm
%{__install} lib/GRNOC/Simp/Poller.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{__install} lib/GRNOC/Simp/Data/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{__install} lib/GRNOC/Simp/CompData/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/CompData/Worker.pm
%{__install} lib/GRNOC/Simp/Poller/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
%{__install} bin/simp.pl %{buildroot}/usr/bin/simp.pl
%{__install} bin/simpData.pl %{buildroot}/usr/bin/simpData.pl
%{__install} bin/compData.pl %{buildroot}/usr/bin/compData.pl
%{__install} conf/*.xml %{buildroot}/etc/grnoc/simp/
%{__install} conf/*.conf %{buildroot}/etc/grnoc/simp/
%{__install} conf/simp.init %{buildroot}/etc/init.d/simp
%{__install} conf/simp_data.init %{buildroot}/etc/init.d/simpData
%{__install} conf/comp_data.init %{buildroot}/etc/init.d/compData

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{perl_vendorlib}/GRNOC/Simp.pm
%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{perl_vendorlib}/GRNOC/Simp/CompData.pm
%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/CompData/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
/usr/bin/simp.pl
/usr/bin/simpData.pl
/usr/bin/compData.pl
/etc/init.d/simp
/etc/init.d/simpData
/etc/init.d/compData
%config(noreplace) /etc/grnoc/simp/config.xml
%config(noreplace) /etc/grnoc/simp/hosts.xml
%config(noreplace) /etc/grnoc/simp/logging.conf
%config(noreplace) /etc/grnoc/simp/simpDataConfig.xml
%config(noreplace) /etc/grnoc/simp/compDataConfig.xml

%doc


%changelog
* Fri Jan 20 2017 Andrew Ragusa <aragusa@aj-dev6.grnoc.iu.edu> - simp-1
- Initial build.

