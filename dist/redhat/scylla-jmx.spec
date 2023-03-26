Name:           %{product}-jmx
Version:        %{version}
Release:        %{release}%{?dist}
Summary:        Scylla JMX
Group:          Applications/Databases

License:        AGPLv3
URL:            http://www.scylladb.com/
Source0:        %{reloc_pkg}

BuildArch:      noarch
BuildRequires:  systemd-units
Requires:       %{product}-server jre-11-headless
AutoReqProv:    no

%description


%prep
%setup -q -n scylla-jmx


%build

%install
./install.sh --packaging --root "$RPM_BUILD_ROOT"

%pre
/usr/sbin/groupadd scylla 2> /dev/null || :
/usr/sbin/useradd -g scylla -s /sbin/nologin -r -d ${_sharedstatedir}/scylla scylla 2> /dev/null || :
ping -c1 `hostname` > /dev/null 2>&1
if [ $? -ne 0 ]; then
echo
echo "**************************************************************"
echo "* WARNING: You need to add hostname on /etc/hosts, otherwise *"
echo "*          scylla-jmx will not able to start up.             *"
echo "**************************************************************"
echo
fi

%post
if [ $1 -eq 1 ] ; then
    /usr/bin/systemctl preset scylla-jmx.service ||:
fi

/usr/bin/systemctl daemon-reload ||:

%preun
if [ $1 -eq 0 ] ; then
    /usr/bin/systemctl --no-reload disable scylla-jmx.service ||:
    /usr/bin/systemctl stop scylla-jmx.service ||:
fi

%postun
/usr/bin/systemctl daemon-reload ||:

%clean
rm -rf $RPM_BUILD_ROOT


%files
%defattr(-,root,root)

%config(noreplace) %{_sysconfdir}/sysconfig/scylla-jmx
%{_unitdir}/scylla-jmx.service
/opt/scylladb/jmx/scylla-jmx
/opt/scylladb/jmx/scylla-jmx-1.0.jar
/opt/scylladb/jmx/symlinks/scylla-jmx
%{_prefix}/lib/scylla/jmx/scylla-jmx
%{_prefix}/lib/scylla/jmx/scylla-jmx-1.0.jar
%{_prefix}/lib/scylla/jmx/symlinks/scylla-jmx

%changelog
* Fri Aug  7 2015 Takuya ASADA Takuya ASADA <syuu@cloudius-systems.com>
- inital version of scylla-tools.spec
