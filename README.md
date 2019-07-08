# Scylla JMX Server

Scylla JMX server implements the Apache Cassandra JMX interface for compatibility with tooling such as `nodetool`. The JMX server uses Scylla's REST API to communicate with a Scylla server.

## Compiling

To compile JMX server, run:

```console
$ mvn --file scylla-jmx-parent/pom.xml package
```

## Running

To start the JMX server, run:

```console
$ ./scripts/scylla-jmx
```

To get help on supported options:

```console
$ ./scripts/scylla-jmx --help
```
