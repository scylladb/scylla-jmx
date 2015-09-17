#!/bin/sh -e

RPMBUILD=build/rpmbuild

if [ ! -e dist/redhat/build_rpm.sh ]; then
    echo "run build_rpm.sh in top of scylla-jmx dir"
    exit 1
fi
if [ ! -f /usr/bin/git ] || [ ! -f /usr/bin/mock ] || [ ! -f /usr/bin/rpmbuild ]; then
    sudo yum install -y git mock rpm-build
fi
VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
git archive --format=tar --prefix=scylla-jmx-$SCYLLA_VERSION/ HEAD -o build/rpmbuild/SOURCES/scylla-jmx-$VERSION.tar
cp dist/redhat/scylla-jmx.spec.in $RPMBUILD/SPECS/scylla-jmx.spec
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" $RPMBUILD/SPECS/scylla-jmx.spec
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" $RPMBUILD/SPECS/scylla-jmx.spec
rpmbuild -bs --define "_topdir $RPMBUILD" -ba $RPMBUILD/SPECS/scylla-jmx.spec
mock rebuild --resultdir=`pwd`/build/rpms $RPMBUILD/SRPMS/scylla-jmx-$VERSION*.src.rpm
