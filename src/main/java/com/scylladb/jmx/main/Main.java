/*
 * Copyright 2015 Cloudius Systems
 */
package com.scylladb.jmx.main;

import com.scylladb.jmx.api.APIConfig;
import com.scylladb.jmx.utils.RMIServerSocketFactoryImpl;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.GCInspector;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

public class Main {

    public static void main(String[] args) throws Exception {
        APIConfig.setConfig();
        System.out.println("Connecting to " + APIConfig.getBaseUrl());
        System.out.println("Starting the JMX server");
        RMIServerSocketFactoryImpl.maybeInitJmx();
        StorageService.getInstance();
        StorageProxy.getInstance();
        MessagingService.getInstance();
        CommitLog.getInstance();
        Gossiper.getInstance();
        EndpointSnitchInfo.getInstance();
        FailureDetector.getInstance();
        CacheService.getInstance();
        CompactionManager.getInstance();
        GCInspector.register();
        Thread.sleep(Long.MAX_VALUE);
    }

}
