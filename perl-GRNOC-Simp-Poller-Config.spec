Summary: Config-file parsing for simp-poller and related programs
Name: perl-GRNOC-Simp-Poller-Config
Version: 1.0.6
Release: 1%{dist}
License: GRNOC
Group: GRNOC
URL: http://globalnoc.iu.edu/simp
Source0: poller-config-%{version}.tar.gz

BuildRequires: perl
BuildRequires: perl(Test::Deep)
BuildRequires: perl(Test::More)
BuildRequires: perl(Test::Pod) >= 1.22

Requires: perl(GRNOC::Config)

Provides: perl(GRNOC::Simp::Poller::Config)
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description

%prep
%setup -q -n poller-config-%{version}

%build

%install
rm -rf $RPM_BUILD_ROOT
%{__install} -d -p %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller

ls -lR lib
%{__install} lib/GRNOC/Simp/Poller/Config.pm %{buildroot}%{perl_vendorlib}/GRNOC/Simp/Poller/Config.pm

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(644,root,root,755)
%{perl_vendorlib}/GRNOC/Simp/Poller/Config.pm

%changelog
