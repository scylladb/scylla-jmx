#!/bin/bash
#
# Copyright (C) 2019 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#

set -e

print_usage() {
    cat <<EOF
Usage: install.sh [options]

Options:
  --root /path/to/root     alternative install root (default /)
  --prefix /prefix         directory prefix (default /usr)
  --nonroot                shortcut of '--disttype nonroot'
  --sysconfdir /etc/sysconfig   specify sysconfig directory name
  --packaging               use install.sh for packaging
  --help                   this helpful message
EOF
    exit 1
}

root=/
sysconfdir=/etc/sysconfig
nonroot=false
packaging=false

while [ $# -gt 0 ]; do
    case "$1" in
        "--root")
            root="$2"
            shift 2
            ;;
        "--prefix")
            prefix="$2"
            shift 2
            ;;
        "--nonroot")
            nonroot=true
            shift 1
            ;;
        "--sysconfdir")
            sysconfdir="$2"
            shift 2
            ;;
        "--packaging")
            packaging=true
            shift 1
            ;;
        "--help")
            shift 1
	    print_usage
            ;;
        *)
            print_usage
            ;;
    esac
done

if ! $packaging; then
    has_java=false
    if [ -x /usr/bin/java ]; then
        javaver=$(/usr/bin/java -version 2>&1|head -n1|cut -f 3 -d " ")
        if [[ "$javaver" =~ ^\"1.8.0 || "$javaver" =~ ^\"11.0. ]]; then
            has_java=true
        fi
    fi
    if ! $has_java; then
        echo "Please install openjdk-8 or openjdk-11 before running install.sh."
        exit 1
    fi
fi

if [ -z "$prefix" ]; then
    if $nonroot; then
        prefix=~/scylladb
    else
        prefix=/opt/scylladb
    fi
fi

rprefix=$(realpath -m "$root/$prefix")
if ! $nonroot; then
    retc="$root/etc"
    rsysconfdir="$root/$sysconfdir"
    rusr="$root/usr"
    rsystemd="$rusr/lib/systemd/system"
else
    retc="$rprefix/etc"
    rsysconfdir="$rprefix/$sysconfdir"
    rsystemd="$retc/systemd"
fi

install -d -m755 "$rsysconfdir"
install -d -m755 "$rsystemd"
install -d -m755 "$rprefix/scripts" "$rprefix/jmx" "$rprefix/jmx/symlinks"

install -m644 dist/common/sysconfig/scylla-jmx -Dt "$rsysconfdir"
install -m644 dist/common/systemd/scylla-jmx.service  -Dt "$rsystemd"
if ! $nonroot; then
    if [ "$sysconfdir" != "/etc/sysconfig" ]; then
        install -d -m755 "$retc"/systemd/system/scylla-jmx.service.d
        cat << EOS > "$retc"/systemd/system/scylla-jmx.service.d/sysconfdir.conf
[Service]
EnvironmentFile=
EnvironmentFile=$sysconfdir/scylla-jmx
EOS
    fi
else
    install -d -m755 "$retc"/systemd/system/scylla-jmx.service.d
    cat << EOS > "$retc"/systemd/system/scylla-jmx.service.d/nonroot.conf
[Service]
EnvironmentFile=
EnvironmentFile=$retc/sysconfig/scylla-jmx
ExecStart=
ExecStart=$rprefix/jmx/scylla-jmx \$SCYLLA_JMX_PORT \$SCYLLA_API_PORT \$SCYLLA_API_ADDR \$SCYLLA_JMX_ADDR \$SCYLLA_JMX_FILE \$SCYLLA_JMX_LOCAL \$SCYLLA_JMX_REMOTE \$SCYLLA_JMX_DEBUG
User=
Group=
WorkingDirectory=
EOS
    if [ ! -d ~/.config/systemd/user/scylla-jmx.service.d ]; then
        mkdir -p ~/.config/systemd/user/scylla-jmx.service.d
    fi
    ln -srf $rsystemd/scylla-jmx.service ~/.config/systemd/user/
    ln -srf "$retc"/systemd/system/scylla-jmx.service.d/nonroot.conf ~/.config/systemd/user/scylla-jmx.service.d
fi

install -m644 scylla-jmx-1.0.jar "$rprefix/jmx"
install -m755 scylla-jmx "$rprefix/jmx"
ln -sf /usr/bin/java "$rprefix/jmx/symlinks/scylla-jmx"
if ! $nonroot; then
    install -m755 -d "$rusr"/lib/scylla/jmx/symlinks
    ln -srf "$rprefix"/jmx/scylla-jmx-1.0.jar "$rusr"/lib/scylla/jmx/
    ln -srf "$rprefix"/jmx/scylla-jmx "$rusr"/lib/scylla/jmx/
    ln -sf /usr/bin/java "$rusr"/lib/scylla/jmx/symlinks/scylla-jmx
fi

if $nonroot; then
    sed -i -e "s#/var/lib/scylla#$rprefix#g" "$rsysconfdir"/scylla-jmx
    sed -i -e "s#/etc/scylla#$rprefix/etc/scylla#g" "$rsysconfdir"/scylla-jmx
    sed -i -e "s#/opt/scylladb/jmx#$rprefix/jmx#g" "$rsysconfdir"/scylla-jmx
    systemctl --user daemon-reload
    echo "Scylla-JMX non-root install completed."
elif ! $packaging; then
    systemctl --system daemon-reload
fi
