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

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.core.MultivaluedMap;

import com.cloudius.urchin.api.APIClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * This module is responsible for Gossiping information for the local endpoint.
 * This abstraction maintains the list of live and dead endpoints. Periodically
 * i.e. every 1 second this module chooses a random node and initiates a round
 * of Gossip with it. A round of Gossip involves 3 rounds of messaging. For
 * instance if node A wants to initiate a round of Gossip with node B it starts
 * off by sending node B a GossipDigestSynMessage. Node B on receipt of this
 * message sends node A a GossipDigestAckMessage. On receipt of this message
 * node A sends node B a GossipDigestAck2Message which completes a round of
 * Gossip. This module as and when it hears one of the three above mentioned
 * messages updates the Failure Detector with the liveness information. Upon
 * hearing a GossipShutdownMessage, this module will instantly mark the remote
 * node as down in the Failure Detector.
 */

public class Gossiper implements GossiperMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";

    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(Gossiper.class.getName());

    private APIClient c = new APIClient();

    public void log(String str) {
        logger.info(str);
    }

    private static final Gossiper instance = new Gossiper();

    public static Gossiper getInstance() {
        return instance;
    }

    private Gossiper() {

        // Register this instance with JMX
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long getEndpointDowntime(String address) throws UnknownHostException {
        log(" getEndpointDowntime(String address) throws UnknownHostException");
        return c.getLongValue("gossiper/downtime/" + address);
    }

    public int getCurrentGenerationNumber(String address)
            throws UnknownHostException {
        log(" getCurrentGenerationNumber(String address) throws UnknownHostException");
        return c.getIntValue("gossiper/generation_number/" + address);
    }

    public void unsafeAssassinateEndpoint(String address)
            throws UnknownHostException {
        log(" unsafeAssassinateEndpoint(String address) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("unsafe", "True");
        c.post("gossiper/assassinate/" + address, queryParams);
    }

    public void assassinateEndpoint(String address) throws UnknownHostException {
        log(" assassinateEndpoint(String address) throws UnknownHostException");
        c.post("gossiper/assassinate/" + address, null);
    }

}
