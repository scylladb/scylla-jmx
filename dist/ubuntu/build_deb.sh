#!/bin/bash -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

if [ -e debian ] || [ -e build ] || [ -e target ] || [ -e m2 ] || [ -e dependency-reduced-pom.xml ]; then
    rm -rf debian build target m2 dependency-reduced-pom.xml
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
if [ "$SCYLLA_VERSION" = "development" ]; then
	SCYLLA_VERSION=0development
fi
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-jmx ../scylla-jmx_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/ubuntu/debian debian
cp dist/ubuntu/changelog.in debian/changelog
sed -i -e "s/@@VERSION@@/$SCYLLA_VERSION/g" debian/changelog
sed -i -e "s/@@RELEASE@@/$SCYLLA_RELEASE/g" debian/changelog

sudo apt-get -y install debhelper maven openjdk-7-jdk devscripts
debuild -r fakeroot -us -uc
