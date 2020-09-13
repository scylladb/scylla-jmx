#!/bin/bash -e

print_usage() {
    echo "build_deb.sh --reloc-pkg build/scylla-jmx-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify Debian package build path"
    exit 1
}

RELOC_PKG=$(readlink -f build/scylla-jmx-package.tar.gz)
BUILDDIR=build/debian
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            OPTS="$OPTS $1 $(readlink -f $2)"
            RELOC_PKG=$2
            shift 2
            ;;
        "--builddir")
            BUILDDIR="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [[ ! $OPTS =~ --reloc-pkg ]]; then
    OPTS="$OPTS --reloc-pkg $RELOC_PKG"
fi
rm -rf "$BUILDDIR"
mkdir -p "$BUILDDIR"/scylla-package
tar -C "$BUILDDIR"/scylla-package -xpf $RELOC_PKG
cd "$BUILDDIR"/scylla-package

RELOC_PKG=$(readlink -f $RELOC_PKG)

mv scylla-jmx/debian debian
PKG_NAME=$(dpkg-parsechangelog --show-field Source)
# XXX: Drop revision number from version string.
#      Since it always '1', this should be okay for now.
PKG_VERSION=$(dpkg-parsechangelog --show-field Version |sed -e 's/-1$//')
ln -fv $RELOC_PKG ../"$PKG_NAME"_"$PKG_VERSION".orig.tar.gz
debuild -rfakeroot -us -uc
