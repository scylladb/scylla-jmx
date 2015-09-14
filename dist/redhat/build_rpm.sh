#!/bin/sh -e

SCYLLA_VER=0.00
RPMBUILD=build/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla-jmx dir"
    exit 1
fi
if [ ! -f /usr/bin/git ] || [ ! -f /usr/bin/mock ] || [ ! -f /usr/bin/rpmbuild ]; then
    sudo yum install -y git mock rpm-build
fi
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
git archive --format=tar --prefix=scylla-jmx-$SCYLLA_VER/ HEAD -o build/rpmbuild/SOURCES/scylla-jmx-$SCYLLA_VER.tar
rpmbuild -bs --define "_topdir $RPMBUILD" -ba dist/redhat/scylla-jmx.spec
mock rebuild --resultdir=`pwd`/build/rpms $RPMBUILD/SRPMS/scylla-jmx-$SCYLLA_VER*.src.rpm
