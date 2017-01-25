/*
 * Copyright 2015 Cloudius Systems
 */
package com.scylladb.jmx.main;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static java.util.Arrays.asList;

import java.lang.reflect.Constructor;

import javax.management.MBeanServer;

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

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to " + config.getBaseUrl());
        System.out.println("Starting the JMX server");

        MBeanServer server = getPlatformMBeanServer();
        for (Class<? extends APIMBean> clazz : asList(StorageService.class, StorageProxy.class, MessagingService.class,
                CommitLog.class, Gossiper.class, EndpointSnitchInfo.class, FailureDetector.class, CacheService.class,
                CompactionManager.class, GCInspector.class, StreamManager.class)) {
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

        String jmxPort = System.getProperty("com.sun.management.jmxremote.port");
        System.out.println("JMX is enabled to receive remote connections on port: " + jmxPort);

        for (;;) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }

}
