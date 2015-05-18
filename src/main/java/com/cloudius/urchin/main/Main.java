/*
 * Copyright 2015 Cloudius Systems
 */
package com.cloudius.urchin.main;

import com.cloudius.urchin.api.APIClient;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to " + APIClient.getBaseUrl());
        System.out.println("Starting the JMX server");
        StorageService.getInstance();
        MessagingService.getInstance();
        Thread.sleep(Long.MAX_VALUE);
    }

}
