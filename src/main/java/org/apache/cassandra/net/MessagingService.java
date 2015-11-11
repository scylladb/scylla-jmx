/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
package org.apache.cassandra.net;

import java.lang.management.ManagementFactory;
import java.net.*;
import java.util.*;
import java.util.Map.Entry;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.cloudius.urchin.api.APIClient;

public final class MessagingService implements MessagingServiceMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(MessagingService.class.getName());

    private APIClient c = new APIClient();

    private final ObjectName jmxObjectName;

    public void log(String str) {
        System.out.println(str);
        logger.info(str);
    }

    public MessagingService() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxObjectName = new ObjectName(MBEAN_NAME);
            mbs.registerMBean(this, jmxObjectName);
            // mbs.registerMBean(StreamManager.instance, new ObjectName(
            // StreamManager.OBJECT_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static MessagingService instance = new MessagingService();

    public static MessagingService getInstance() {
        return instance;
    }

    /**
     * Pending tasks for Command(Mutations, Read etc) TCP Connections
     */
    public Map<String, Integer> getCommandPendingTasks() {
        log(" getCommandPendingTasks()");
        return c.getMapStringIntegerValue("/messaging_service/messages/pending");
    }

    /**
     * Completed tasks for Command(Mutations, Read etc) TCP Connections
     */
    public Map<String, Long> getCommandCompletedTasks() {
        System.out.println("getCommandCompletedTasks!");
        Map<String, Long> res = c
                .getListMapStringLongValue("/messaging_service/messages/sent");
        return res;
    }

    /**
     * Dropped tasks for Command(Mutations, Read etc) TCP Connections
     */
    public Map<String, Long> getCommandDroppedTasks() {
        log(" getCommandDroppedTasks()");
        return c.getMapStringLongValue("/messaging_service/messages/dropped");
    }

    /**
     * Pending tasks for Response(GOSSIP & RESPONSE) TCP Connections
     */
    public Map<String, Integer> getResponsePendingTasks() {
        log(" getResponsePendingTasks()");
        return c.getMapStringIntegerValue("/messaging_service/messages/respond_pending");
    }

    /**
     * Completed tasks for Response(GOSSIP & RESPONSE) TCP Connections
     */
    public Map<String, Long> getResponseCompletedTasks() {
        log(" getResponseCompletedTasks()");
        return c.getMapStringLongValue("/messaging_service/messages/respond_completed");
    }

    /**
     * dropped message counts for server lifetime
     */
    public Map<String, Integer> getDroppedMessages() {
        log(" getDroppedMessages()");
        return c.getMapStringIntegerValue("/messaging_service/messages/dropped");
    }

    /**
     * dropped message counts since last called
     */
    public Map<String, Integer> getRecentlyDroppedMessages() {
        log(" getRecentlyDroppedMessages()");
        return c.getMapStringIntegerValue("");
    }

    /**
     * Total number of timeouts happened on this node
     */
    public long getTotalTimeouts() {
        log(" getTotalTimeouts()");
        Map<String, Long> timeouts = getTimeoutsPerHost();
        long res = 0;
        for (Entry<String, Long> t : timeouts.entrySet()) {
            res += t.getValue();
        }
        return res;
    }

    /**
     * Number of timeouts per host
     */
    public Map<String, Long> getTimeoutsPerHost() {
        log(" getTimeoutsPerHost()");
        return c.getMapStringLongValue("/messaging_service/messages/timeout");
    }

    /**
     * Number of timeouts since last check.
     */
    public long getRecentTotalTimouts() {
        log(" getRecentTotalTimouts()");
        return c.getLongValue("");
    }

    /**
     * Number of timeouts since last check per host.
     */
    public Map<String, Long> getRecentTimeoutsPerHost() {
        log(" getRecentTimeoutsPerHost()");
        return c.getMapStringLongValue("");
    }

    public int getVersion(String address) throws UnknownHostException {
        log(" getVersion(String address) throws UnknownHostException");
        return c.getIntValue("");
    }

}
