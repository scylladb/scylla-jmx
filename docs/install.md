# Install Scylla JMX Package

scylla-jmx becomes optional package from ScyllaDB 6.2, not installed by
default. If you need JMX server you can still install it from scylla-jmx
GitHub page.

## Debian/Ubuntu

1.  Download .deb package from scylla-jmx page.

    Select latest release from "releases", download a file end with ".deb".

2.  (Optional) Transfer the downloaded package to the install node.

    If the pc from which you downloaded the package is different from
    the node where you install scylladb, you will need to transfer the
    files to the node.

3.  Install scylla-jmx package.

    ```
    sudo apt install -y ./scylla-jmx_<version>_all.deb
    ```

## Centos/RHEL

1.  Download .rpm package from scylla-jmx page.

    Select latest release from "releases", download a file end with ".rpm".

2.  (Optional) Transfer the downloaded package to the install node.

    If the pc from which you downloaded the package is different from
    the node where you install scylladb, you will need to transfer the
    files to the node.

3.  Install scylla-jmx package.

    ```
    sudo yum install -y ./scylla-jmx-<version>.noarch.rpm
    ```


## Install without root privileges

1.  Download .tar.gz package from scylla-jmx page.

    Select latest release from "releases", download a file end with ".tar.gz".

2.  (Optional) Transfer the downloaded package to the install node.

    If the pc from which you downloaded the package is different from
    the node where you install scylladb, you will need to transfer the
    files to the node.

3.  Install scylla-jmx package.

    ```
    tar xpf scylla-jmx-<version>.noarch.tar.gz
    cd scylla-jmx
    ./install.sh --nonroot
    ```
