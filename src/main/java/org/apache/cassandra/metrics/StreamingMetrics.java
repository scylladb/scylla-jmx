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
 * Copyright 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import javax.json.JsonArray;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMetrics;
import com.scylladb.jmx.metrics.DefaultNameFactory;
import com.scylladb.jmx.metrics.MetricNameFactory;
import com.yammer.metrics.core.Counter;

/**
 * Metrics for streaming.
 */
public class StreamingMetrics
{
    public static final String TYPE_NAME = "Streaming";
    private static final Map<String, StreamingMetrics> instances = new HashMap<String, StreamingMetrics>();
    static final int INTERVAL = 1000; //update every 1second

    private static Timer timer = new Timer("Streaming Metrics");

    public static final Counter activeStreamsOutbound = APIMetrics.newCounter("/stream_manager/metrics/outbound", DefaultNameFactory.createMetricName(TYPE_NAME, "ActiveOutboundStreams", null));
    public static final Counter totalIncomingBytes = APIMetrics.newCounter("/stream_manager/metrics/incoming", DefaultNameFactory.createMetricName(TYPE_NAME, "TotalIncomingBytes", null));
    public static final Counter totalOutgoingBytes = APIMetrics.newCounter("/stream_manager/metrics/outgoing", DefaultNameFactory.createMetricName(TYPE_NAME, "TotalOutgoingBytes", null));
    public final Counter incomingBytes;
    public final Counter outgoingBytes;
    private static APIClient s_c = new APIClient();

    public static void register_mbeans() {
        TimerTask taskToExecute = new CheckRegistration();
        timer.scheduleAtFixedRate(taskToExecute, 100, INTERVAL);
    }

    public StreamingMetrics(final InetAddress peer)
    {
        MetricNameFactory factory = new DefaultNameFactory("Streaming", peer.getHostAddress().replaceAll(":", "."));
        incomingBytes = APIMetrics.newCounter("/stream_manager/metrics/incoming/" + peer,factory.createMetricName("IncomingBytes"));
        outgoingBytes= APIMetrics.newCounter("/stream_manager/metrics/outgoing/" + peer, factory.createMetricName("OutgoingBytes"));
    }

    public static boolean checkRegistration() {
        try {
            JsonArray streams = s_c.getJsonArray("/stream_manager/");
            Set<String> all = new HashSet<String>();
            for (int i = 0; i < streams.size(); i ++) {
                JsonArray sessions = streams.getJsonObject(i).getJsonArray("sessions");
                for (int j = 0; j < sessions.size(); j++) {
                    String name = sessions.getJsonObject(j).getString("peer");
                    if (!instances.containsKey(name)) {
                        StreamingMetrics metrics = new StreamingMetrics(InetAddress.getByName(name));
                        instances.put(name, metrics);
                    }
                    all.add(name);
                }
            }
            //removing deleted stream
            for (String n : instances.keySet()) {
                if (! all.contains(n)) {
                    instances.remove(n);
                }
            }
        } catch (Exception e) {
            // ignoring exceptions, will retry on the next interval
            return false;
        }
        return true;
    }

    private static final class CheckRegistration extends TimerTask {
        @Override
        public void run() {
            checkRegistration();
        }
    }
}
