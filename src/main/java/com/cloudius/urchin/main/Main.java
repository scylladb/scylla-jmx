/*
 * Copyright 2015 Cloudius Systems
 */
package com.cloudius.urchin.main;

import com.cloudius.urchin.api.APIConfig;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

public class Main {

    public static void main(String[] args) throws Exception {
        APIConfig.setConfig();
        System.out.println("Connecting to " + APIConfig.getBaseUrl());
        System.out.println("Starting the JMX server");
        StorageService.getInstance();
        StorageProxy.getInstance();
        MessagingService.getInstance();
        CommitLog.getInstance();
        Gossiper.getInstance();
        EndpointSnitchInfo.getInstance();
        FailureDetector.getInstance();
        ColumnFamilyStore.register_mbeans();
        CacheService.getInstance();
        CompactionManager.getInstance();
        Thread.sleep(Long.MAX_VALUE);
    }

}
