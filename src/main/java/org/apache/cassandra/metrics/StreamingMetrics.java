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

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.cassandra.metrics.DefaultNameFactory.createMetricName;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import javax.json.JsonArray;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMBean;
import com.sun.jmx.mbeanserver.JmxMBeanServer;

/**
 * Metrics for streaming.
 */
public class StreamingMetrics {
    public static final String TYPE_NAME = "Streaming";

    private static final HashSet<ObjectName> globalNames;

    static {
        try {
            globalNames = new HashSet<ObjectName>(asList(createMetricName(TYPE_NAME, "ActiveOutboundStreams", null),
                    createMetricName(TYPE_NAME, "TotalIncomingBytes", null),
                    createMetricName(TYPE_NAME, "TotalOutgoingBytes", null)));
        } catch (MalformedObjectNameException e) {
            throw new Error(e);
        }
    };

    private StreamingMetrics() {
    }

    private static boolean isStreamingName(ObjectName n) {
        return TYPE_NAME.equals(n.getKeyProperty("type"));
    }

    public static void unregister(APIClient client, JmxMBeanServer server) throws MalformedObjectNameException {
        APIMBean.checkRegistration(server, emptySet(), StreamingMetrics::isStreamingName, (n) -> null);
    }

    public static boolean checkRegistration(APIClient client, JmxMBeanServer server)
            throws MalformedObjectNameException, UnknownHostException {

        Set<ObjectName> all = new HashSet<ObjectName>(globalNames);
        JsonArray streams = client.getJsonArray("/stream_manager/");
        for (int i = 0; i < streams.size(); i++) {
            JsonArray sessions = streams.getJsonObject(i).getJsonArray("sessions");
            for (int j = 0; j < sessions.size(); j++) {
                String peer = sessions.getJsonObject(j).getString("peer");
                String scope = InetAddress.getByName(peer).getHostAddress().replaceAll(":", ".");
                all.add(createMetricName(TYPE_NAME, "IncomingBytes", scope));
                all.add(createMetricName(TYPE_NAME, "OutgoingBytes", scope));
            }
        }

        MetricsRegistry registry = new MetricsRegistry(client, server);
        return APIMBean.checkRegistration(server, all, StreamingMetrics::isStreamingName, n -> {
            String scope = n.getKeyProperty("scope");
            String name = n.getKeyProperty("name");

            String url = null;
            if ("ActiveOutboundStreams".equals(name)) {
                url = "/stream_manager/metrics/outbound";
            } else if ("IncomingBytes".equals(name) || "TotalIncomingBytes".equals(name)) {
                url = "/stream_manager/metrics/incoming";
            } else if ("OutgoingBytes".equals(name) || "TotalOutgoingBytes".equals(name)) {
                url = "/stream_manager/metrics/outgoing";
            }
            if (url == null) {
                throw new IllegalArgumentException();
            }
            if (scope != null) {
                url = url + "/" + scope;
            }
            return registry.counter(url);
        });
    }
}
