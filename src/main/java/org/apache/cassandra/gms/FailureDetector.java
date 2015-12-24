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

import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.*;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.scylladb.jmx.api.APIClient;

public class FailureDetector implements FailureDetectorMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=FailureDetector";
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(FailureDetector.class.getName());

    private APIClient c = new APIClient();

    public void log(String str) {
        logger.info(str);
    }

    private static final FailureDetector instance = new FailureDetector();

    public static FailureDetector getInstance() {
        return instance;
    }

    private FailureDetector() {
        // Register this instance with JMX
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void dumpInterArrivalTimes() {
        log(" dumpInterArrivalTimes()");
    }

    public void setPhiConvictThreshold(double phi) {
        log(" setPhiConvictThreshold(double phi)");
    }

    public double getPhiConvictThreshold() {
        log(" getPhiConvictThreshold()");
        return c.getDoubleValue("/failure_detector/phi");
    }

    public String getAllEndpointStates() {
        log(" getAllEndpointStates()");

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, EndpointState> entry : getEndpointStateMap().entrySet())
        {
            sb.append('/').append(entry.getKey()).append("\n");
            appendEndpointState(sb, entry.getValue());
        }
        return sb.toString();
    }

    private void appendEndpointState(StringBuilder sb, EndpointState endpointState)
    {
        sb.append("  generation:").append(endpointState.getHeartBeatState().getGeneration()).append("\n");
        sb.append("  heartbeat:").append(endpointState.getHeartBeatState().getHeartBeatVersion()).append("\n");
        for (Map.Entry<ApplicationState, String> state : endpointState.applicationState.entrySet())
        {
            if (state.getKey() == ApplicationState.TOKENS)
                continue;
            sb.append("  ").append(state.getKey()).append(":").append(state.getValue()).append("\n");
        }
    }

    public Map<String, EndpointState> getEndpointStateMap() {
        Map<String, EndpointState> res = new HashMap<String, EndpointState>();
        JsonArray arr = c.getJsonArray("/failure_detector/endpoints");
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

    public String getEndpointState(String address) throws UnknownHostException {
        log(" getEndpointState(String address) throws UnknownHostException");
        return c.getStringValue("/failure_detector/endpoints/states/" +  address);
    }

    public Map<String, String> getSimpleStates() {
        log(" getSimpleStates()");
        return c.getMapStrValue("/failure_detector/simple_states");
    }

    public int getDownEndpointCount() {
        log(" getDownEndpointCount()");
        return c.getIntValue("/failure_detector/count/endpoint/down");
    }

    public int getUpEndpointCount() {
        log(" getUpEndpointCount()");
        return c.getIntValue("/failure_detector/count/endpoint/up");
    }

}
