#!/bin/bash -e

. /etc/os-release

print_usage() {
    echo "build_reloc.sh --clean --nodeps"
    echo "  --clean clean build directory"
    echo "  --nodeps    skip installing dependencies"
    exit 1
}

CLEAN=
NODEPS=
while [ $# -gt 0 ]; do
    case "$1" in
        "--clean")
            CLEAN=yes
            shift 1
            ;;
        "--nodeps")
            NODEPS=yes
            shift 1
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


if [ ! -e reloc/build_reloc.sh ]; then
    echo "run build_reloc.sh in top of scylla dir"
    exit 1
fi

if [ "$CLEAN" = "yes" ]; then
    rm -rf build target
fi

if [ -f build/scylla-jmx-package.tar.gz ]; then
    rm build/scylla-jmx-package.tar.gz
fi

if [ -z "$NODEPS" ]; then
    sudo ./install-dependencies.sh
fi

mvn -B --file scylla-jmx-parent/pom.xml install
./SCYLLA-VERSION-GEN
./dist/debian/debian_files_gen.py
scripts/create-relocatable-package.py build/scylla-jmx-package.tar.gz
