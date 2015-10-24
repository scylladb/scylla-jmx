#!/bin/sh -e

if [ ! -e dist/ubuntu/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi

sudo apt-get -y install debhelper maven openjdk-7-jdk

debuild -r fakeroot --no-tgz-check -us -uc
