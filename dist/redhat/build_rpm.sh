#!/bin/sh -e

SCYLLA_VER=0.00
RPMBUILD=~/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla-jmx dir"
    exit 1
fi
if [ ! -f /usr/bin/git ] || [ ! -f /usr/bin/yum-builddep ] || [ ! -f /usr/bin/rpmbuild ]; then
    sudo yum install -y yum-utils git rpm-build rpmdevtools
fi
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
curdir=`basename $(pwd)`
cd ..
if [ "$curdir" != "scylla-jmx-$SCYLLA_VER" ]; then
    echo "WARNING: base directory name should be 'scylla-jmx-$SCYLLA_VER'"
    ln -s $curdir scylla-jmx-$SCYLLA_VER
fi
tar --exclude-vcs --exclude-vcs-ignores -cpf $RPMBUILD/SOURCES/scylla-jmx-$SCYLLA_VER.tar scylla-jmx-$SCYLLA_VER $curdir
if [ "$curdir" != "scylla-jmx-$SCYLLA_VER" ]; then
    rm scylla-jmx-$SCYLLA_VER
fi
cd -
cp dist/redhat/scylla-jmx.spec $RPMBUILD/SPECS
sudo yum-builddep -y $RPMBUILD/SPECS/scylla-jmx.spec
rpmbuild --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/scylla-jmx.spec
