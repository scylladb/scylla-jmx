#!/bin/bash -e

. /etc/os-release
print_usage() {
    echo "build_rpm.sh --reloc-pkg build/scylla-jmx-package.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    exit 1
}
RELOC_PKG=$(readlink -f build/scylla-jmx-package.tar.gz)
OPTS=""
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            OPTS="$OPTS $1 $(readlink -f $2)"
            RELOC_PKG=$2
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
mkdir -p build/redhat
tar -C build/redhat/ -xpf $RELOC_PKG scylla-jmx-package/SCYLLA-RELEASE-FILE scylla-jmx-package/SCYLLA-RELOCATABLE-FILE scylla-jmx-package/SCYLLA-VERSION-FILE scylla-jmx-package/SCYLLA-PRODUCT-FILE scylla-jmx-package/dist/redhat
cd build/redhat/scylla-jmx-package
exec ./dist/redhat/build_rpm.sh $OPTS
