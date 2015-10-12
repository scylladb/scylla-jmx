#!/bin/sh -e

RPMBUILD=`pwd`/build/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla-jmx dir"
    exit 1
fi

sudo yum install -y rpm-build git

OS=`awk '{print $1}' /etc/redhat-release`
if [ "$OS" = "Fedora" ] && [ ! -f /usr/bin/mock ]; then
    sudo yum -y install mock
elif [ "$OS" = "CentOS" ] && [ ! -f /usr/bin/yum-builddep ]; then
    sudo yum -y install yum-utils
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
git archive --format=tar --prefix=scylla-jmx-$SCYLLA_VERSION/ HEAD -o build/rpmbuild/SOURCES/scylla-jmx-$VERSION.tar
cp dist/redhat/scylla-jmx.spec.in $RPMBUILD/SPECS/scylla-jmx.spec
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" $RPMBUILD/SPECS/scylla-jmx.spec
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" $RPMBUILD/SPECS/scylla-jmx.spec

if [ "$OS" = "Fedora" ]; then
    rpmbuild -bs --define "_topdir $RPMBUILD" $RPMBUILD/SPECS/scylla-jmx.spec
    /usr/bin/mock rebuild --resultdir=`pwd`/build/rpms $RPMBUILD/SRPMS/scylla-jmx-$VERSION*.src.rpm
else
    sudo yum-builddep -y  $RPMBUILD/SPECS/scylla-jmx.spec
    rpmbuild -ba --define "_topdir $RPMBUILD" $RPMBUILD/SPECS/scylla-jmx.spec
fi
