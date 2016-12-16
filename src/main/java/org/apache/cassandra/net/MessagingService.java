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

import static java.util.Collections.emptyMap;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.apache.cassandra.metrics.DroppedMessageMetrics;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.MetricsMBean;

public final class MessagingService extends MetricsMBean implements MessagingServiceMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=MessagingService";
    private static final Logger logger = Logger.getLogger(MessagingService.class.getName());

    private Map<String, Long> resentTimeouts = new HashMap<String, Long>();
    private long recentTimeoutCount;

    /* All verb handler identifiers */
    public enum Verb {
        MUTATION, @Deprecated BINARY, READ_REPAIR, READ, REQUEST_RESPONSE, // client-initiated
                                                                           // reads
                                                                           // and
                                                                           // writes
        @Deprecated STREAM_INITIATE, @Deprecated STREAM_INITIATE_DONE, @Deprecated STREAM_REPLY, @Deprecated STREAM_REQUEST, RANGE_SLICE, @Deprecated BOOTSTRAP_TOKEN, @Deprecated TREE_REQUEST, @Deprecated TREE_RESPONSE, @Deprecated JOIN, GOSSIP_DIGEST_SYN, GOSSIP_DIGEST_ACK, GOSSIP_DIGEST_ACK2, @Deprecated DEFINITIONS_ANNOUNCE, DEFINITIONS_UPDATE, TRUNCATE, SCHEMA_CHECK, @Deprecated INDEX_SCAN, REPLICATION_FINISHED, INTERNAL_RESPONSE, // responses
                                                                                                                                                                                                                                                                                                                                                                                                                                                       // to
                                                                                                                                                                                                                                                                                                                                                                                                                                                       // internal
                                                                                                                                                                                                                                                                                                                                                                                                                                                       // calls
        COUNTER_MUTATION, @Deprecated STREAMING_REPAIR_REQUEST, @Deprecated STREAMING_REPAIR_RESPONSE, SNAPSHOT, // Similar
                                                                                                                 // to
                                                                                                                 // nt
                                                                                                                 // snapshot
        MIGRATION_REQUEST, GOSSIP_SHUTDOWN, _TRACE, // dummy verb so we can use
                                                    // MS.droppedMessages
        ECHO, REPAIR_MESSAGE,
        // use as padding for backwards compatability where a previous version
        // needs to validate a verb from the future.
        PAXOS_PREPARE, PAXOS_PROPOSE, PAXOS_COMMIT, PAGED_RANGE,
        // remember to add new verbs at the end, since we serialize by ordinal
        UNUSED_1, UNUSED_2, UNUSED_3,;
    }

    public void log(String str) {
        logger.finest(str);
    }

    public MessagingService(APIClient client) {
        super(MBEAN_NAME, client,
                Stream.of(Verb.values()).map(v -> new DroppedMessageMetrics(v)).collect(Collectors.toList()));
    }

    /**
     * Pending tasks for Command(Mutations, Read etc) TCP Connections
     */
    @Override
    public Map<String, Integer> getCommandPendingTasks() {
        log(" getCommandPendingTasks()");
        return client.getMapStringIntegerValue("/messaging_service/messages/pending");
    }

    /**
     * Completed tasks for Command(Mutations, Read etc) TCP Connections
     */
    @Override
    public Map<String, Long> getCommandCompletedTasks() {
        log("getCommandCompletedTasks()");
        Map<String, Long> res = client.getListMapStringLongValue("/messaging_service/messages/sent");
        return res;
    }

    /**
     * Dropped tasks for Command(Mutations, Read etc) TCP Connections
     */
    @Override
    public Map<String, Long> getCommandDroppedTasks() {
        log(" getCommandDroppedTasks()");
        return client.getMapStringLongValue("/messaging_service/messages/dropped");
    }

    /**
     * Pending tasks for Response(GOSSIP & RESPONSE) TCP Connections
     */
    @Override
    public Map<String, Integer> getResponsePendingTasks() {
        log(" getResponsePendingTasks()");
        return client.getMapStringIntegerValue("/messaging_service/messages/respond_pending");
    }

    /**
     * Completed tasks for Response(GOSSIP & RESPONSE) TCP Connections
     */
    @Override
    public Map<String, Long> getResponseCompletedTasks() {
        log(" getResponseCompletedTasks()");
        return client.getMapStringLongValue("/messaging_service/messages/respond_completed");
    }

    /**
     * dropped message counts for server lifetime
     */
    @Override
    public Map<String, Integer> getDroppedMessages() {
        log(" getDroppedMessages()");
        Map<String, Integer> res = new HashMap<String, Integer>();
        JsonArray arr = client.getJsonArray("/messaging_service/messages/dropped_by_ver");
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            res.put(obj.getString("verb"), obj.getInt("count"));
        }
        return res;
    }

    private Map<String, Integer> recent;

    /**
     * dropped message counts since last called
     */
    @Override
    public Map<String, Integer> getRecentlyDroppedMessages() {
        log(" getRecentlyDroppedMessages()");

        Map<String, Integer> dropped = getDroppedMessages(), result = new HashMap<>(dropped), old = recent;

        recent = dropped;

        if (old != null) {
            for (Map.Entry<String, Integer> e : old.entrySet()) {
                result.put(e.getKey(), result.get(e.getKey()) - e.getValue());
            }
        }

        return result;
    }

    /**
     * Total number of timeouts happened on this node
     */
    @Override
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
    @Override
    public Map<String, Long> getTimeoutsPerHost() {
        log(" getTimeoutsPerHost()");
        return client.getMapStringLongValue("/messaging_service/messages/timeout");
    }

    /**
     * Number of timeouts since last check.
     */
    @Override
    public long getRecentTotalTimouts() {
        log(" getRecentTotalTimouts()");
        long timeoutCount = getTotalTimeouts();
        long recent = timeoutCount - recentTimeoutCount;
        recentTimeoutCount = timeoutCount;
        return recent;
    }

    /**
     * Number of timeouts since last check per host.
     */
    @Override
    public Map<String, Long> getRecentTimeoutsPerHost() {
        log(" getRecentTimeoutsPerHost()");
        Map<String, Long> timeouts = getTimeoutsPerHost();
        Map<String, Long> result = new HashMap<String, Long>();
        for (Entry<String, Long> e : timeouts.entrySet()) {
            long res = e.getValue().longValue()
                    - ((resentTimeouts.containsKey(e.getKey())) ? (resentTimeouts.get(e.getKey())).longValue() : 0);
            resentTimeouts.put(e.getKey(), e.getValue());
            result.put(e.getKey(), res);
        }
        return result;
    }

    @Override
    public int getVersion(String address) throws UnknownHostException {
        log(" getVersion(String address) throws UnknownHostException");
        return client.getIntValue("");
    }

    @Override
    public Map<String, Integer> getLargeMessagePendingTasks() {
        // TODO: implement for realsies
        return getCommandPendingTasks();
    }

    @Override
    public Map<String, Long> getLargeMessageCompletedTasks() {
        // TODO: implement for realsies
        return getCommandCompletedTasks();
    }

    @Override
    public Map<String, Long> getLargeMessageDroppedTasks() {
        // TODO: implement for realsies
        return getCommandDroppedTasks();
    }

    @Override
    public Map<String, Integer> getSmallMessagePendingTasks() {
        // TODO: implement for realsies
        return getResponsePendingTasks();
    }

    @Override
    public Map<String, Long> getSmallMessageCompletedTasks() {
        // TODO: implement for realsies
        return getResponseCompletedTasks();
    }

    @Override
    public Map<String, Long> getSmallMessageDroppedTasks() {
        // TODO: implement for realsies
        return emptyMap();
    }

    @Override
    public Map<String, Integer> getGossipMessagePendingTasks() {
        // TODO: implement for realsies
        return emptyMap();
    }

    @Override
    public Map<String, Long> getGossipMessageCompletedTasks() {
        // TODO: implement for realsies
        return emptyMap();
    }

    @Override
    public Map<String, Long> getGossipMessageDroppedTasks() {
        // TODO: implement for realsies
        return emptyMap();
    }
}
