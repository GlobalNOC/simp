Name: simp-monitor
Version: 1.0.7
Release: 1%{?dist}
Summary: A functionality to monitor SIMP
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: simp-monitor-%{version}.tar.gz


BuildRequires: perl
BuildRequires: perl(Test::Deep)
BuildRequires: perl(Test::More)
BuildRequires: perl(Test::Pod) >= 1.22
BuildRequires: rabbitmq-server

Requires: redis
Requires: perl(AnyEvent)
Requires: perl-GRNOC-Log
Requires: perl-GRNOC-Config
Requires: perl-GRNOC-RabbitMQ >= 1.1.1
Requires: perl(POSIX)
Requires: perl-Redis >= 1.991

%description


%prep
%setup -q -n simp-monitor-%{version}


%build

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}/usr/bin
%{__install} -d -p %{buildroot}/etc/simp
%{__install} -d -p %{buildroot}/etc/rsyslog.d
# %{__install} -d -p %{buildroot}/var/www/html
%{__install} -d -p %{buildroot}/var/log
%{__install} -d -p %{buildroot}/etc/httpd/conf.d
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap/css
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap/js
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/css
# %{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/js

%{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap/css
%{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap/js
%{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/css
%{__install} -d -p %{buildroot}/usr/share/grnoc/simp-monitor/assets/js

%{__install} -d -p %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin


%{__install} bin/response_timer.pl %{buildroot}/usr/bin/response_timer.pl
%{__install} conf/response-graph-config.xml %{buildroot}/etc/simp/
%{__install} conf/response-monitor-log.conf %{buildroot}/etc/rsyslog.d/
%{__install} conf/simp_monitor_http.conf %{buildroot}/etc/httpd/conf.d/

%{__install} cgi-dir/comp.cgi %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/comp.cgi
%{__install} cgi-dir/get_poller_data.cgi  %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/get_poller_data.cgi
%{__install} cgi-dir/populate_data.cgi  %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/populate_data.cgi
%{__install} cgi-dir/simp.cgi  %{buildroot}/usr/lib/grnoc/simp-monitor/cgi-bin/simp.cgi

%{__install} simp/composite.html  %{buildroot}/usr/share/grnoc/simp-monitor/composite.html
%{__install} simp/data.html  %{buildroot}/usr/share/grnoc/simp-monitor/data.html
%{__install} simp/index.html  %{buildroot}/usr/share/grnoc/simp-monitor/index.html
%{__install} simp/redis.html  %{buildroot}/usr/share/grnoc/simp-monitor/redis.html
%{__install} simp/assets/bootstrap/css/bootstrap.min.css  %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap/css/bootstrap.min.css
%{__install} simp/assets/bootstrap/js/bootstrap.min.js  %{buildroot}/usr/share/grnoc/simp-monitor/assets/bootstrap/js/bootstrap.min.js
%{__install} simp/assets/css/user.css  %{buildroot}/usr/share/grnoc/simp-monitor/assets/css/user.css
%{__install} simp/assets/js/jquery.min.js  %{buildroot}/usr/share/grnoc/simp-monitor/assets/js/jquery.min.js
%{__install} simp/assets/js/redis.js  %{buildroot}/usr/share/grnoc/simp-monitor/assets/js/redis.js

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(755,root,root,755)
/usr/bin/response_timer.pl
/usr/lib/grnoc/simp-monitor/cgi-bin/comp.cgi
/usr/lib/grnoc/simp-monitor/cgi-bin/get_poller_data.cgi
/usr/lib/grnoc/simp-monitor/cgi-bin/populate_data.cgi
/usr/lib/grnoc/simp-monitor/cgi-bin/simp.cgi
/usr/share/grnoc/simp-monitor/composite.html
/usr/share/grnoc/simp-monitor/data.html
/usr/share/grnoc/simp-monitor/index.html
/usr/share/grnoc/simp-monitor/redis.html
/usr/share/grnoc/simp-monitor/assets/bootstrap/css/bootstrap.min.css
/usr/share/grnoc/simp-monitor/assets/bootstrap/js/bootstrap.min.js
/usr/share/grnoc/simp-monitor/assets/css/user.css
/usr/share/grnoc/simp-monitor/assets/js/jquery.min.js
/usr/share/grnoc/simp-monitor/assets/js/redis.js
%defattr(644,root,root,755)
%config(noreplace) /etc/simp/response-graph-config.xml
%config(noreplace) /etc/rsyslog.d/response-monitor-log.conf
%config(noreplace) /etc/httpd/conf.d/simp_monitor_http.conf
%doc

%post
mkfifo /var/log/response_pipe
%changelog

