#!/bin/bash -e

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

SCYLLA_VERSION=$(cat SCYLLA-VERSION-FILE)
SCYLLA_RELEASE=$(cat SCYLLA-RELEASE-FILE)
VERSION=$SCYLLA_VERSION-$SCYLLA_RELEASE
PRODUCT=$(cat SCYLLA-PRODUCT-FILE)

RPMBUILD=$(readlink -f ../)
mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

ln -fv $RELOC_PKG $RPMBUILD/SOURCES/

parameters=(
    -D"version $SCYLLA_VERSION"
    -D"release $SCYLLA_RELEASE"
    -D"product $PRODUCT"
)

cp dist/redhat/scylla-jmx.spec $RPMBUILD/SPECS
# this rpm can be install on both fedora / centos7, so drop distribution name from the file name
rpmbuild -ba "${parameters[@]}" --define '_binary_payload w2.xzdio' --define "_topdir $RPMBUILD" --undefine "dist" $RPM_JOBS_OPTS $RPMBUILD/SPECS/scylla-jmx.spec
