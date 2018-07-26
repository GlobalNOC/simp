Summary: A program to collect data from redis and send it to tsds 
Name: simp-monitor
Version: 1.0.7
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-monitor-%{version}.tar.gz

BuildRequires: perl

Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config

Provides: perl(GRNOC::Simp::Redis_Tsds)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n simp-monitor-%{version}

%build

%pre
/usr/bin/getent group simp || /usr/sbin/groupadd -r simp
/usr/bin/getent passwd simp || /usr/sbin/useradd -r -s /sbin/nologin -g simp simp

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}/etc/init.d
%{__install} -d -p %{buildroot}/etc/simp/hosts.d
%{__install} -d -p %{buildroot}/usr/bin/
%{__install} -d -p %{buildroot}/etc/simp
%{__install} -d -p %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin
%{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor

%{__install} bin/redis_tsds.pl %{buildroot}/usr/bin/redis_tsds.pl
%{__install} bin/response_timer.pl %{buildroot}/usr/bin/response_timer.pl
%{__install} lib/GRNOC/Simp/comp.cgi  %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/comp.cgi 
%{__install} lib/GRNOC/Simp/get_poller_data.cgi   %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/get_poller_data.cgi
%{__install} lib/GRNOC/Simp/populate_data.cgi   %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/populate_data.cgi
%{__install} lib/GRNOC/Simp/simp.cgi   %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/simp.cgi
%{__install} lib/GRNOC/Simp/test.cgi %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/test.cgi
cp -a lib/GRNOC/Simp/web/*  %{buildroot}/usr/share/grnoc/simp-monitor
%{__install} conf/redis_config.xml %{buildroot}/etc/simp/redis_config.xml
%{__install} conf/redis_log.conf %{buildroot}/etc/simp/redis_log.conf
%{__install) conf/redis_tsds_cron %{buildroot}/etc/cron.d/redis_tsds_cron
%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(755,root,root,755)
/usr/bin/redis_tsds.pl
/usr/lib/grnoc/simp-monitor
/usr/share/grnoc/simp-monitor
/usr/bin/response_timer.pl
/etc/cron.d/redis_tsds_cron
%defattr(644,root,root,755)
%config(noreplace) /etc/simp/redis_log.conf
%config(noreplace) /etc/simp/redis_config.xml

%doc


%changelog
* Mon Jul 23 2018 Paritosh Morparia <pmorpari@globalnoc.iu.edu> - 1.0.0
  - Initial build




