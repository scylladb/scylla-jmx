/*
 * Copyright 2015 Cloudius Systems
 */
package com.scylladb.jmx.main;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.rmi.registry.LocateRegistry.createRegistry;
import static java.util.Arrays.asList;
import static javax.net.ServerSocketFactory.getDefault;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.StreamManager;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.api.APIConfig;
import com.scylladb.jmx.metrics.APIMBean;

public class Main {
    // todo: command line options. Make us an agent class (also)
    private static final APIConfig config = new APIConfig();
    public static final APIClient client = new APIClient(config);

    private static JMXConnectorServer jmxServer = null;

    private static void setupJmx() {
        System.setProperty("javax.management.builder.initial", "com.scylladb.jmx.utils.APIBuilder");
        String jmxPort = System.getProperty("com.sun.management.jmxremote.port");

        if (jmxPort == null) {
            System.out.println("JMX is not enabled to receive remote connections.");

            jmxPort = System.getProperty("cassandra.jmx.local.port", "7199");
            String address = System.getProperty("jmx.address", "localhost");
            if (address.equals("localhost")) {
                System.setProperty("java.rmi.server.hostname", InetAddress.getLoopbackAddress().getHostAddress());
            } else {
                try {
                    System.setProperty("java.rmi.server.hostname", InetAddress.getByName(address).getHostAddress());
                } catch (UnknownHostException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
            try {
                RMIServerSocketFactory serverFactory = pPort -> getDefault().createServerSocket(pPort, 0,
                        InetAddress.getLoopbackAddress());
                createRegistry(Integer.valueOf(jmxPort), null, serverFactory);

                StringBuffer url = new StringBuffer();
                url.append("service:jmx:");
                url.append("rmi://").append(address).append("/jndi/");
                url.append("rmi://").append(address).append(":").append(jmxPort).append("/jmxrmi");
                System.out.println(url);
                Map<String, Object> env = new HashMap<>();
                env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, serverFactory);

                jmxServer = new RMIConnectorServer(new JMXServiceURL(url.toString()), env, getPlatformMBeanServer());

                jmxServer.start();
            } catch (IOException e) {
                System.out.println("Error starting local jmx server: " + e.toString());
            }

        } else {
            System.out.println("JMX is enabled to receive remote connections on port: " + jmxPort);
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to " + config.getBaseUrl());
        System.out.println("Starting the JMX server");

        setupJmx();

        try {
            MBeanServer server = getPlatformMBeanServer();
            for (Class<? extends APIMBean> clazz : asList(StorageService.class, StorageProxy.class,
                    MessagingService.class, CommitLog.class, Gossiper.class, EndpointSnitchInfo.class,
                    FailureDetector.class, CacheService.class, CompactionManager.class, GCInspector.class,
                    StreamManager.class)) {
                Constructor<? extends APIMBean> c = clazz.getDeclaredConstructor(APIClient.class);
                APIMBean m = c.newInstance(client);
                server.registerMBean(m, null);
            }

            try {
                // forces check for dynamically created mbeans
                server.queryNames(null, null);
            } catch (IllegalStateException e) {
                // ignore this. Just means we started before scylla.
            }

            for (;;) {
                Thread.sleep(Long.MAX_VALUE);
            }
        } finally {
            // make sure to kill the server otherwise we can hang. Not an issue
            // when killed perhaps, but any exception above etc would leave a
            // zombie.
            if (jmxServer != null) {
                jmxServer.stop();
            }
        }
    }

}
