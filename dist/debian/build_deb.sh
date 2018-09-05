#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_deb.sh -target <codename>"
    echo "  --target target distribution codename"
    exit 1
}
TARGET=
while [ $# -gt 0 ]; do
    case "$1" in
        "--target")
            TARGET=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
is_debian_variant() {
    [ -f /etc/debian_version ]
}
is_debian() {
    case "$1" in
        jessie|stretch) return 0;;
        *) return 1;;
    esac
}
is_ubuntu() {
    case "$1" in
        trusty|xenial|bionic) return 0;;
        *) return 1;;
    esac
}


pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    elif is_debian_variant; then
        sudo apt-get install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

if [ ! -e dist/debian/build_deb.sh ]; then
    echo "run build_deb.sh in top of scylla dir"
    exit 1
fi
if [ "$(arch)" != "x86_64" ]; then
    echo "Unsupported architecture: $(arch)"
    exit 1
fi

if [ -e debian ] || [ -e build/release ]; then
    sudo rm -rf debian build
    mkdir build
fi
if is_debian_variant; then
    sudo apt-get -y update
fi
# this hack is needed since some environment installs 'git-core' package, it's
# subset of the git command and doesn't works for our git-archive-all script.
if is_redhat_variant && [ ! -f /usr/libexec/git-core/git-submodule ]; then
    sudo yum install -y git
fi
if [ ! -f /usr/bin/git ]; then
    pkg_install git
fi
if [ ! -f /usr/bin/python ]; then
    pkg_install python
fi
if [ ! -f /usr/sbin/pbuilder ]; then
    pkg_install pbuilder
fi
if [ ! -f /usr/bin/mvn ]; then
    pkg_install maven
fi
if [ ! -f /usr/bin/dh_testdir ]; then
    pkg_install debhelper
fi
if [ ! -f /usr/bin/pystache ]; then
    if is_redhat_variant; then
        sudo yum install -y /usr/bin/pystache
    elif is_debian_variant; then
        sudo apt-get install -y python-pystache
    fi
fi


if [ -z "$TARGET" ]; then
    if is_debian_variant; then
        if [ ! -f /usr/bin/lsb_release ]; then
            pkg_install lsb-release
        fi
        TARGET=`lsb_release -c|awk '{print $2}'`
    else
        echo "Please specify target"
        exit 1
    fi
fi

VERSION=$(./SCYLLA-VERSION-GEN)
SCYLLA_VERSION=$(cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/')
SCYLLA_RELEASE=$(cat build/SCYLLA-RELEASE-FILE)
echo $VERSION > version
./scripts/git-archive-all --extra version --force-submodules --prefix scylla-jmx ../scylla-jmx_$SCYLLA_VERSION-$SCYLLA_RELEASE.orig.tar.gz 

cp -a dist/debian/debian debian
if is_debian $TARGET; then
    REVISION="1~$TARGET"
elif is_ubuntu $TARGET; then
    REVISION="0ubuntu1~$TARGET"
else
   echo "Unknown distribution: $TARGET"
fi

MUSTACHE_DIST="\"debian\": true, \"$TARGET\": true"
pystache dist/debian/changelog.mustache "{ \"version\": \"$SCYLLA_VERSION\", \"release\": \"$SCYLLA_RELEASE\", \"revision\": \"$REVISION\", \"codename\": \"$TARGET\" }" > debian/changelog
pystache dist/debian/rules.mustache "{ $MUSTACHE_DIST }" > debian/rules
chmod a+rx debian/rules

if [ "$TARGET" != "trusty" ]; then
    pystache dist/common/systemd/scylla-jmx.service.mustache "{ $MUSTACHE_DIST }" > debian/scylla-jmx.service
fi

sudo rm -fv /var/cache/pbuilder/scylla-jmx-$TARGET.tgz
sudo -E DIST=$TARGET /usr/sbin/pbuilder clean --configfile ./dist/debian/pbuilderrc
sudo -E DIST=$TARGET /usr/sbin/pbuilder create --configfile ./dist/debian/pbuilderrc
sudo -E DIST=$TARGET /usr/sbin/pbuilder update --configfile ./dist/debian/pbuilderrc
if [ "$TARGET" = "jessie" ]; then
    echo "apt-get install -y -t jessie-backports ca-certificates-java" > build/jessie-pkginst.sh
    chmod a+rx build/jessie-pkginst.sh
    sudo -E DIST=$TARGET /usr/sbin/pbuilder --configfile ./dist/debian/pbuilderrc execute build/jessie-pkginst.sh
elif [ "$TARGET" = "bionic" ]; then
    echo "apt-get install -y ca-certificates-java openjdk-8-jdk-headless" > build/bionic-workaround.sh
    echo "update-ca-certificates -f" >> build/bionic-workaround.sh
    chmod a+rx build/bionic-workaround.sh
    sudo -E DIST=$TARGET /usr/sbin/pbuilder --configfile ./dist/debian/pbuilderrc execute --save-after-exec build/bionic-workaround.sh
fi
sudo -E DIST=$TARGET pdebuild --configfile ./dist/debian/pbuilderrc --buildresult build/debs
