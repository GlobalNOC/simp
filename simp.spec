%global debug_package %{nil} # Don't generate debug info
%global _binaries_in_noarch_packages_terminate_build   0
AutoReqProv: no # Keep rpmbuild from trying to figure out Perl on its own

Summary: A small system for gathering large amounts of snmp data.
Name: simp
Version: 1.11.3
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-GRNOC-RabbitMQ >= 1.2.1
Requires: perl-GRNOC-WebService-Client
Requires: perl-GRNOC-Monitoring-Service-Status

Provides: perl(GRNOC::Simp::Comp)
Provides: perl(GRNOC::Simp::Comp::Worker)
Provides: perl(GRNOC::Simp::Data)
Provides: perl(GRNOC::Simp::Data::Worker)
Provides: perl(GRNOC::Simp::Poller)
Provides: perl(GRNOC::Simp::Poller::Worker)
Provides: perl(GRNOC::Simp::TSDS)
Provides: perl(GRNOC::Simp::TSDS::Master)
Provides: perl(GRNOC::Simp::TSDS::Pusher)
Provides: perl(GRNOC::Simp::TSDS::Worker)

%description

%pre 
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%post
systemctl daemon-reload

%prep
%setup -q -n simp-%{version}

%build

%install
rm -rf %{buildroot}
%{__install} -d -p %{buildroot}/etc/simp
%{__install} -d -p %{buildroot}/etc/simp/comp
%{__install} -d -p %{buildroot}/etc/simp/comp/composites.d
%{__install} -d -p %{buildroot}/etc/simp/comp/validation.d
%{__install} -d -p %{buildroot}/etc/simp/data
%{__install} -d -p %{buildroot}/etc/simp/data/validation.d
%{__install} -d -p %{buildroot}/etc/simp/poller
%{__install} -d -p %{buildroot}/etc/simp/poller/hosts.d/
%{__install} -d -p %{buildroot}/etc/simp/poller/groups.d/
%{__install} -d -p %{buildroot}/etc/simp/poller/validation.d/
%{__install} -d -p %{buildroot}/etc/simp/tsds
%{__install} -d -p %{buildroot}/etc/simp/tsds/collections.d/
%{__install} -d -p %{buildroot}/etc/simp/tsds/validation.d/
%{__install} -d -p %{buildroot}/etc/systemd/system/
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Comp
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/TSDS
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/var/lib/simp/poller/
%{__install} -d -p %{buildroot}/var/lib/grnoc/simp-tsds/workers

%{__install} bin/simp-test.pl %{buildroot}/usr/bin/simp-test.pl
%{__install} bin/simp-comp.pl %{buildroot}/usr/bin/simp-comp.pl
%{__install} bin/simp-data.pl %{buildroot}/usr/bin/simp-data.pl
%{__install} bin/simp-poller.pl %{buildroot}/usr/bin/simp-poller.pl
%{__install} bin/simp-tsds.pl %{buildroot}/usr/bin

%{__install} conf/comp/config.xml %{buildroot}/etc/simp/comp/config.xml
%{__install} conf/comp/config.xsd %{buildroot}/etc/simp/comp/validation.d/config.xsd
%{__install} conf/data/config.xml %{buildroot}/etc/simp/data/config.xml
%{__install} conf/data/config.xsd %{buildroot}/etc/simp/data/validation.d/config.xsd
%{__install} conf/poller/config.xml %{buildroot}/etc/simp/poller/config.xml
%{__install} conf/poller/config.xsd %{buildroot}/etc/simp/poller/validation.d/config.xsd
%{__install} conf/tsds/config.xml %{buildroot}/etc/simp/tsds/config.xml
%{__install} conf/tsds/config.xsd %{buildroot}/etc/simp/tsds/validation.d/config.xsd

%{__install} conf/logging.conf %{buildroot}/etc/simp/comp/logging.conf
%{__install} conf/logging.conf %{buildroot}/etc/simp/data/logging.conf
%{__install} conf/logging.conf %{buildroot}/etc/simp/poller/logging.conf
%{__install} conf/logging.conf %{buildroot}/etc/simp/tsds/logging.conf

%{__install} conf/comp/composite.xml.example %{buildroot}/etc/simp/comp/composites.d/composite.xml.example
%{__install} conf/comp/composite.xsd %{buildroot}/etc/simp/comp/validation.d/composite.xsd
%{__install} conf/poller/hosts.xml.example %{buildroot}/etc/simp/poller/hosts.d/hosts.xml.example
%{__install} conf/poller/group.xml.example %{buildroot}/etc/simp/poller/groups.d/group.xml.example
%{__install} conf/poller/hosts.xsd %{buildroot}/etc/simp/poller/validation.d/hosts.xsd
%{__install} conf/poller/group.xsd %{buildroot}/etc/simp/poller/validation.d/group.xsd
%{__install} conf/tsds/collection.xml.example %{buildroot}/etc/simp/tsds/collections.d/collection.xml.example
%{__install} conf/tsds/collection.xsd %{buildroot}/etc/simp/tsds/validation.d/collection.xsd

%{__install} conf/comp/simp-comp.systemd %{buildroot}/etc/systemd/system/simp-comp.service
%{__install} conf/data/simp-data.systemd %{buildroot}/etc/systemd/system/simp-data.service
%{__install} conf/poller/simp-poller.systemd %{buildroot}/etc/systemd/system/simp-poller.service
%{__install} conf/tsds/simp-tsds.systemd %{buildroot}/etc/systemd/system/simp-tsds.service

%{__install} lib/GRNOC/Simp/Comp.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Comp.pm
%{__install} lib/GRNOC/Simp/Comp/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Comp/Worker.pm
%{__install} lib/GRNOC/Simp/Data.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{__install} lib/GRNOC/Simp/Data/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{__install} lib/GRNOC/Simp/Poller.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{__install} lib/GRNOC/Simp/Poller/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
%{__install} lib/GRNOC/Simp/TSDS.pm %{buildroot}/%{perl_vendorlib}/GRNOC/Simp/
%{__install} lib/GRNOC/Simp/TSDS/Master.pm %{buildroot}/%{perl_vendorlib}/GRNOC/Simp/TSDS/
%{__install} lib/GRNOC/Simp/TSDS/Worker.pm %{buildroot}/%{perl_vendorlib}/GRNOC/Simp/TSDS/
%{__install} lib/GRNOC/Simp/TSDS/Pusher.pm %{buildroot}/%{perl_vendorlib}/GRNOC/Simp/TSDS/

# clean up buildroot
find %{buildroot} -name .packlist -exec %{__rm} {} \;

%{_fixperms} %{buildroot}/*

%clean
rm -rf %{buildroot}

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Comp.pm
%{perl_vendorlib}/GRNOC/Simp/Comp/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/Poller.pm
%{perl_vendorlib}/GRNOC/Simp/Poller/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Master.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Worker.pm
%{perl_vendorlib}/GRNOC/Simp/TSDS/Pusher.pm

%defattr(755,root,root,755)
/usr/bin/simp-comp.pl
/usr/bin/simp-data.pl
/usr/bin/simp-poller.pl
/usr/bin/simp-test.pl
/usr/bin/simp-tsds.pl


%defattr(644,root,root,644)
/etc/simp/comp/validation.d/config.xsd
/etc/simp/comp/validation.d/composite.xsd
/etc/simp/data/validation.d/config.xsd
/etc/simp/poller/validation.d/config.xsd
/etc/simp/poller/validation.d/hosts.xsd
/etc/simp/poller/validation.d/group.xsd
/etc/simp/tsds/validation.d/config.xsd
/etc/simp/tsds/validation.d/collection.xsd
/etc/systemd/system/simp-comp.service
/etc/systemd/system/simp-data.service
/etc/systemd/system/simp-poller.service
/etc/systemd/system/simp-tsds.service

%defattr(644,root,root,755)
%config(noreplace) /etc/simp/comp/logging.conf
%config(noreplace) /etc/simp/comp/config.xml
%config(noreplace) /etc/simp/comp/composites.d/*
%config(noreplace) /etc/simp/data/config.xml
%config(noreplace) /etc/simp/data/logging.conf
%config(noreplace) /etc/simp/poller/config.xml
%config(noreplace) /etc/simp/poller/hosts.d/*
%config(noreplace) /etc/simp/poller/groups.d/*
%config(noreplace) /etc/simp/poller/logging.conf
%config(noreplace) /etc/simp/tsds/config.xml
%config(noreplace) /etc/simp/tsds/logging.conf
%config(noreplace) /etc/simp/tsds/collections.d/*

%defattr(755,simp,simp,755)
%dir /var/lib/simp/
%dir /var/lib/simp/poller/
%dir /var/lib/grnoc/simp-tsds
%dir /var/lib/grnoc/simp-tsds/workers
