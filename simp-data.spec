Summary: A small system for fetching SNMP data from redis and returning it via RabbitMQ
Name: simp-data
Version: 1.6.0
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-data-%{version}.tar.gz

# This prevents automatic dependency resolution from failing in external imports.
# Without it, the created RPMs may not install properly
AutoReqProv: no

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
Requires: perl-GRNOC-RabbitMQ >= 1.2.1
Requires: perl-Moo
Requires: perl-Parallel-ForkManager
Requires: perl(POSIX)
Requires: perl-Redis-Fast >= 0.28
Requires: perl-Syntax-Keyword-Try
Requires: perl-Type-Tiny
Requires: perl(Class::Accessor::Fast)

Provides: perl(GRNOC::Simp::Data)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-data-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%post
systemctl daemon-reload

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data
%{__install} -d -p %{buildroot}/etc/systemd/system/
%{__install} -d -p %{buildroot}/etc/init.d/
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/simp
%{__install} -d -p %{buildroot}/etc/simp/data
%{__install} -d -p %{buildroot}/etc/simp/data/validation.d

%{__install} lib/GRNOC/Simp/Data.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{__install} lib/GRNOC/Simp/Data/Worker.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm
%{__install} bin/simp-data.pl %{buildroot}/usr/bin/simp-data.pl
%{__install} conf/data/config.xml %{buildroot}/etc/simp/data/config.xml
%{__install} conf/data/config.xsd %{buildroot}/etc/simp/data/validation.d/config.xsd
%{__install} conf/logging.conf %{buildroot}/etc/simp/data/logging.conf

%if 0%{?rhel} >= 7
%{__install} conf/data/simp-data.systemd %{buildroot}/etc/systemd/system/simp-data.service
%else
%{__install} conf/data/simp-data.service %{buildroot}/etc/init.d/simp-data
%endif

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Data.pm
%{perl_vendorlib}/GRNOC/Simp/Data/Worker.pm

%defattr(755,root,root,755)
/usr/bin/simp-data.pl

%defattr(644,root,root,644)
/etc/simp/data/validation.d/config.xsd

%if 0%{?rhel} >= 7
/etc/systemd/system/simp-data.service
%else
/etc/init.d/simp-data
%endif

%defattr(644,root,root,755)
%config(noreplace) /etc/simp/data/config.xml
%config(noreplace) /etc/simp/data/logging.conf

%doc
