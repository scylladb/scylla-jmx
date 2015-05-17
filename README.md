# Urchin JMX Interface
This is the JMX interface for urchin.
## Compile
To compile do:
```
mvn install
```

## Run
The maven will copy relevant jars to your local directory and would
set the classpath accordingly.
```
java -jar target/urchin-mbean-1.0.jar
```

## Setting IP and Port
By default the the JMX would connect to a node on the localhost
on port 10000.

The jmx API uses the system properties to set the IP address and Port.
To change the ip address use the apiaddress property (e.g. -Dapiaddress=1.1.1.1)
To change the port use the apiport (e.g. -Dapiport=10001)
