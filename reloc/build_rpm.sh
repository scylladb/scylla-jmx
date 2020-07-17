#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --reloc-pkg build/scylla-jmx-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --builddir specify rpmbuild directory"
    exit 1
}
RELOC_PKG=$(readlink -f build/scylla-jmx-package.tar.gz)
BUILDDIR=build/redhat
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
mkdir -p "$BUILDDIR"
tar -C "$BUILDDIR" -xpf $RELOC_PKG scylla-jmx/SCYLLA-RELEASE-FILE scylla-jmx/SCYLLA-RELOCATABLE-FILE scylla-jmx/SCYLLA-VERSION-FILE scylla-jmx/SCYLLA-PRODUCT-FILE scylla-jmx/dist/redhat
cd "$BUILDDIR"/scylla-jmx
exec ./dist/redhat/build_rpm.sh $OPTS
