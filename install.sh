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
  --sysconfdir /etc/sysconfig   specify sysconfig directory name
  --help                   this helpful message
EOF
    exit 1
}

root=/
prefix=/opt/scylladb
sysconfdir=/etc/sysconfig

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
        "--sysconfdir")
            sysconfdir="$2"
            shift 2
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

rprefix="$root/$prefix"
retc="$root/etc"
rusr="$root/usr"
rsysconfdir="$root/$sysconfdir"

install -d -m755 "$rsysconfdir"
install -d -m755 "$rusr"/lib/systemd/system
install -d -m755 "$rprefix/scripts" "$rprefix/jmx" "$rprefix/jmx/symlinks"

install -m644 dist/common/sysconfig/scylla-jmx -Dt "$rsysconfdir"
install -m644 dist/common/systemd/scylla-jmx.service  -Dt "$rusr"/lib/systemd/system
if [ "$sysconfdir" != "/etc/sysconfig" ]; then
    install -d -m755 "$retc"/systemd/system/scylla-jmx.service.d
    cat << EOS > "$retc"/systemd/system/scylla-jmx.service.d/sysconfdir.conf
[Service]
EnvironmentFile=
EnvironmentFile=$sysconfdir/scylla-jmx
EOS
fi

install -m644 scylla-jmx-1.0.jar "$rprefix/jmx"
install -m755 scylla-jmx "$rprefix/jmx"
ln -sf /usr/bin/java "$rprefix/jmx/symlinks/scylla-jmx"
# create symlink for /usr/lib/scylla/jmx
ln -srf $rprefix/jmx "$rprefix/scripts/"
