/*
 * Copyright 2015 Cloudius Systems
 */
package com.cloudius.main;

import com.cloudius.api.APIClient;

public class Main {

    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to " + APIClient.getBaseUrl());
        System.out.println("Starting the JMX server");
        Thread.sleep(Long.MAX_VALUE);
    }

}
