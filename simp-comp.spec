Summary: A system for fetching data from simp and compiling the data into a composite
Name: simp-comp
Version: 1.5.0
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
Requires: perl-GRNOC-RabbitMQ >= 1.2.1
Requires: perl-Moo
Requires: perl-Parallel-ForkManager
Requires: perl(POSIX)
Requires: perl-Try-Tiny
Requires: perl-Type-Tiny

Provides: perl(GRNOC::Simp::Comp)
Provides: perl(GRNOC::Simp::Comp::Worker)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-comp-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%post
systemctl daemon-reload

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Comp
%{__install} -d -p %{buildroot}/etc/systemd/system/
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/simp
%{__install} -d -p %{buildroot}/etc/simp/comp
%{__install} -d -p %{buildroot}/etc/simp/comp/composites.d
%{__install} -d -p %{buildroot}/etc/simp/comp/validation.d

%{__install} lib/GRNOC/Simp/Comp.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Comp.pm
%{__install} lib/GRNOC/Simp/Comp/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Comp/Worker.pm
%{__install} bin/simp-comp.pl %{buildroot}/usr/bin/simp-comp.pl
%{__install} conf/comp/config.xml %{buildroot}/etc/simp/comp/config.xml
%{__install} conf/comp/composite.xml.example %{buildroot}/etc/simp/comp/composites.d/composite.xml.example
%{__install} conf/logging.conf %{buildroot}/etc/simp/comp/logging.conf
%{__install} conf/comp/config.xsd %{buildroot}/etc/simp/comp/validation.d/config.xsd
%{__install} conf/comp/composite.xsd %{buildroot}/etc/simp/comp/validation.d/composite.xsd

%if 0%{?rhel} >= 7
%{__install} conf/comp/simp-comp.systemd %{buildroot}/etc/systemd/system/simp-comp.service
%else
%{__install} conf/comp/simp-comp.service %{buildroot}/etc/init.d/simp-comp
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Comp.pm
%{perl_vendorlib}/GRNOC/Simp/Comp/Worker.pm

%defattr(755,root,root,755)
/usr/bin/simp-comp.pl

%defattr(644,root,root,644)
/etc/simp/comp/validation.d/config.xsd
/etc/simp/comp/validation.d/composite.xsd

%if 0%{?rhel} >= 7
/etc/systemd/system/simp-comp.service
%else
/etc/init.d/simp-comp
%endif

%defattr(644,root,root,755)
%config(noreplace) /etc/simp/comp/logging.conf
%config(noreplace) /etc/simp/comp/config.xml
%config(noreplace) /etc/simp/comp/composites.d/*

%doc
