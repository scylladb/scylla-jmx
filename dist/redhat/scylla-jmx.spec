Name:           scylla-jmx
Version:        0.00
Release:        1%{?dist}
Summary:        Scylla JMX
Group:          Applications/Databases

License:        Proprietary
URL:            http://www.seastar-project.org/
Source0:        %{name}-%{version}.tar

BuildArch:      noarch
BuildRequires:  maven java-1.8.0-openjdk
Requires:       maven java-1.8.0-openjdk

%description


%prep
%setup -q


%build
env JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk mvn install

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_unitdir}
mkdir -p $RPM_BUILD_ROOT%{_prefix}/lib/scylla/

install -m644 dist/redhat/systemd/scylla-jmx.service $RPM_BUILD_ROOT%{_unitdir}/
install -d -m755 $RPM_BUILD_ROOT%{_prefix}/lib/scylla
install -d -m755 $RPM_BUILD_ROOT%{_prefix}/lib/scylla/jmx
install -m644 target/urchin-mbean-1.0.jar $RPM_BUILD_ROOT%{_prefix}/lib/scylla/jmx/

%pre
/usr/sbin/groupadd scylla 2> /dev/null || :
/usr/sbin/useradd -g scylla -s /sbin/nologin -r -d ${_sharedstatedir}/scylla scylla 2> /dev/null || :

%post
%systemd_post scylla-jmx.service

%preun
%systemd_preun scylla-jmx.service

%postun
%systemd_postun

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root)

%{_unitdir}/scylla-jmx.service
%{_prefix}/lib/scylla/jmx/urchin-mbean-1.0.jar

%changelog
* Fri Aug  7 2015 Takuya ASADA Takuya ASADA <syuu@cloudius-systems.com>
- inital version of scylla-tools.spec
