# Urchin JMX Interface
This is the JMX interface for urchin.
## Compile
To compile do:
```
mvn install
```

## Run
The maven will create an uber-jar with all dependency under the target directory. You should run it with the remote jmx enable so the nodetool will be able to connect to it.

```
java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=7199 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -jar target/urchin-mbean-1.0.jar
```

## Setting IP and Port
By default the the JMX would connect to a node on the localhost
on port 10000.

The jmx API uses the system properties to set the IP address and Port.
To change the ip address use the apiaddress property (e.g. -Dapiaddress=1.1.1.1)
To change the port use the apiport (e.g. -Dapiport=10001)
