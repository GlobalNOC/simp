Summary: A system for fetching data from simp and compiling the data into a composite
Name: simp-env
Version: 1.12.0
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-env-%{version}.tar.gz
%global debug_package %{nil}
# This prevents automatic dependency resolution from failing in external imports.
# Without it, the created RPMs may not install properly
AutoReqProv: no

BuildRequires: perl
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-GRNOC-RabbitMQ >= 1.2.1
Requires: perl-GRNOC-WebService-Client
Requires: perl-GRNOC-Monitoring-Service-Status

Provides: perl-Redis-Fast = 0.28
Provides: perl(Data::Munge)
Provides: perl-Syntax-Keyword-Try
Provides: perl(AnyEvent::Subprocess)
Provides: perl(MooseX::Clone)
Provides: perl(AnyEvent::SNMP)
Provides: perl-Net-SNMP-XS

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
%{__install} -d -p %{buildroot}/opt/grnoc/venv/simp/lib/perl5
cp -r venv/lib/perl5/* -t %{buildroot}/opt/grnoc/venv/simp/lib/perl5

# clean up buildroot
#find %{buildroot} -name .packlist -exec %{__rm} {} \;

#%{_fixperms} %{buildroot}/*

%clean
rm -rf %{buildroot}

%files
%defattr(-, simp, simp, 755)
/opt/grnoc/venv/simp/lib/perl5/*