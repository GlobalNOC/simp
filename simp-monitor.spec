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

%{__install} -d -p %{buildroot}/var/www/html/simp_monitor/assets/bootstrap/css
%{__install} -d -p %{buildroot}/var/www/html/simp_monitor/assets/bootstrap/js
%{__install} -d -p %{buildroot}/var/www/html/simp_monitor/assets/css
%{__install} -d -p %{buildroot}/var/www/html/simp_monitor/assets/js

%{__install} -d -p %{buildroot}/var/www/html/cgi-dir


%{__install} bin/response_timer.pl %{buildroot}/usr/bin/response_timer.pl
%{__install} conf/response-graph-config.xml %{buildroot}/etc/simp/
%{__install} conf/response-monitor-log.conf %{buildroot}/etc/rsyslog.d/
%{__install} conf/simp_monitor_http.conf %{buildroot}/etc/httpd/conf.d/

%{__install} cgi-dir/comp.cgi %{buildroot}/var/www/html/cgi-dir/comp.cgi
%{__install} cgi-dir/get_poller_data.cgi  %{buildroot}/var/www/html/cgi-dir/get_poller_data.cgi
%{__install} cgi-dir/populate_data.cgi  %{buildroot}/var/www/html/cgi-dir/populate_data.cgi
%{__install} cgi-dir/simp.cgi  %{buildroot}/var/www/html/cgi-dir/simp.cgi

%{__install} simp/composite.html  %{buildroot}/var/www/html/simp_monitor/composite.html
%{__install} simp/data.html  %{buildroot}/var/www/html/simp_monitor/data.html
%{__install} simp/index.html  %{buildroot}/var/www/html/simp_monitor/index.html
%{__install} simp/redis.html  %{buildroot}/var/www/html/simp_monitor/redis.html
%{__install} simp/assets/bootstrap/css/bootstrap.min.css  %{buildroot}/var/www/html/simp_monitor/assets/bootstrap/css/bootstrap.min.css
%{__install} simp/assets/bootstrap/js/bootstrap.min.js  %{buildroot}/var/www/html/simp_monitor/assets/bootstrap/js/bootstrap.min.js
%{__install} simp/assets/css/user.css  %{buildroot}/var/www/html/simp_monitor/assets/css/user.css
%{__install} simp/assets/js/jquery.min.js  %{buildroot}/var/www/html/simp_monitor/assets/js/jquery.min.js
%{__install} simp/assets/js/redis.js  %{buildroot}/var/www/html/simp_monitor/assets/js/redis.js

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(755,root,root,755)
/usr/bin/response_timer.pl
/var/www/html/cgi-dir/comp.cgi
/var/www/html/cgi-dir/get_poller_data.cgi
/var/www/html/cgi-dir/populate_data.cgi
/var/www/html/cgi-dir/simp.cgi
/var/www/html/simp_monitor/composite.html
/var/www/html/simp_monitor/data.html
/var/www/html/simp_monitor/index.html
/var/www/html/simp_monitor/redis.html
/var/www/html/simp_monitor/assets/bootstrap/css/bootstrap.min.css
/var/www/html/simp_monitor/assets/bootstrap/js/bootstrap.min.js
/var/www/html/simp_monitor/assets/css/user.css
/var/www/html/simp_monitor/assets/js/jquery.min.js
/var/www/html/simp_monitor/assets/js/redis.js
%defattr(644,root,root,755)
%config(noreplace) /etc/simp/response-graph-config.xml
%config(noreplace) /etc/rsyslog.d/response-monitor-log.conf
%config(noreplace) /etc/httpd/conf.d/simp_monitor_http.conf
%doc

%post
mkfifo /var/log/response_pipe
%changelog

