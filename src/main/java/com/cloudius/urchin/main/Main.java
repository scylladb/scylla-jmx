/*
 * Copyright 2015 Cloudius Systems
 */
package com.cloudius.urchin.main;

import com.cloudius.urchin.api.APIClient;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.EndpointSnitchInfo;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to " + APIClient.getBaseUrl());
        System.out.println("Starting the JMX server");
        StorageService.getInstance();
        MessagingService.getInstance();
        CommitLog.getInstance();
        Gossiper.getInstance();
        EndpointSnitchInfo.getInstance();
        Thread.sleep(Long.MAX_VALUE);
    }

}
