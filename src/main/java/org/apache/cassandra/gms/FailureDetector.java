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

package org.apache.cassandra.gms;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMBean;

public class FailureDetector extends APIMBean implements FailureDetectorMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=FailureDetector";
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(FailureDetector.class.getName());

    public FailureDetector(APIClient c) {
        super(c);
    }

    public void log(String str) {
        logger.finest(str);
    }

    @Override
    public void dumpInterArrivalTimes() {
        log(" dumpInterArrivalTimes()");
    }

    @Override
    public void setPhiConvictThreshold(double phi) {
        log(" setPhiConvictThreshold(double phi)");
    }

    @Override
    public double getPhiConvictThreshold() {
        log(" getPhiConvictThreshold()");
        return client.getDoubleValue("/failure_detector/phi");
    }

    @Override
    public String getAllEndpointStates() {
        log(" getAllEndpointStates()");

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, EndpointState> entry : getEndpointStateMap().entrySet()) {
            sb.append('/').append(entry.getKey()).append("\n");
            appendEndpointState(sb, entry.getValue());
        }
        return sb.toString();
    }

    private void appendEndpointState(StringBuilder sb, EndpointState endpointState) {
        sb.append("  generation:").append(endpointState.getHeartBeatState().getGeneration()).append("\n");
        sb.append("  heartbeat:").append(endpointState.getHeartBeatState().getHeartBeatVersion()).append("\n");
        for (Map.Entry<ApplicationState, String> state : endpointState.applicationState.entrySet()) {
            if (state.getKey() == ApplicationState.TOKENS) {
                continue;
            }
            sb.append("  ").append(state.getKey()).append(":").append(state.getValue()).append("\n");
        }
    }

    public Map<String, EndpointState> getEndpointStateMap() {
        Map<String, EndpointState> res = new HashMap<String, EndpointState>();
        JsonArray arr = client.getJsonArray("/failure_detector/endpoints");
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            EndpointState ep = new EndpointState(new HeartBeatState(obj.getInt("generation"), obj.getInt("version")));
            ep.setAliave(obj.getBoolean("is_alive"));
            ep.setUpdateTimestamp(obj.getJsonNumber("update_time").longValue());
            JsonArray states = obj.getJsonArray("application_state");
            for (int j = 0; j < states.size(); j++) {
                JsonObject state = states.getJsonObject(j);
                ep.addApplicationState(state.getInt("application_state"), state.getString("value"));
            }
            res.put(obj.getString("addrs"), ep);
        }
        return res;
    }

    @Override
    public String getEndpointState(String address) throws UnknownHostException {
        log(" getEndpointState(String address) throws UnknownHostException");
        return client.getStringValue("/failure_detector/endpoints/states/" + address);
    }

    @Override
    public Map<String, String> getSimpleStates() {
        log(" getSimpleStates()");
        return client.getMapStrValue("/failure_detector/simple_states");
    }

    @Override
    public int getDownEndpointCount() {
        log(" getDownEndpointCount()");
        return client.getIntValue("/failure_detector/count/endpoint/down");
    }

    @Override
    public int getUpEndpointCount() {
        log(" getUpEndpointCount()");
        return client.getIntValue("/failure_detector/count/endpoint/up");
    }

    // From origin:
    // this is useless except to provide backwards compatibility in
    // phi_convict_threshold,
    // because everyone seems pretty accustomed to the default of 8, and users
    // who have
    // already tuned their phi_convict_threshold for their own environments
    // won't need to
    // change.
    private final double PHI_FACTOR = 1.0 / Math.log(10.0); // 0.434...

    @Override
    public TabularData getPhiValues() throws OpenDataException {
        final CompositeType ct = new CompositeType("Node", "Node", new String[] { "Endpoint", "PHI" },
                new String[] { "IP of the endpoint", "PHI value" },
                new OpenType[] { SimpleType.STRING, SimpleType.DOUBLE });
        final TabularDataSupport results = new TabularDataSupport(
                new TabularType("PhiList", "PhiList", ct, new String[] { "Endpoint" }));
        final JsonArray arr = client.getJsonArray("/failure_detector/endpoint_phi_values");

        for (JsonValue v : arr) {
            JsonObject o = (JsonObject) v;
            String endpoint = o.getString("endpoint");
            double phi = Double.parseDouble(o.getString("phi"));

            if (phi != Double.MIN_VALUE) {
                // returned values are scaled by PHI_FACTOR so that the are on
                // the same scale as PhiConvictThreshold
                final CompositeData data = new CompositeDataSupport(ct, new String[] { "Endpoint", "PHI" },
                        new Object[] { endpoint, phi * PHI_FACTOR });
                results.put(data);
            }
        }

        return results;
    }
}
