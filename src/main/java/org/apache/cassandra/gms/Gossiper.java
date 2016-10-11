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
import java.util.logging.Logger;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMBean;

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

public class Gossiper extends APIMBean implements GossiperMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.net:type=Gossiper";

    private static final Logger logger = Logger.getLogger(Gossiper.class.getName());

    public Gossiper(APIClient c) {
        super(c);
    }

    public void log(String str) {
        logger.finest(str);
    }

    @Override
    public long getEndpointDowntime(String address) throws UnknownHostException {
        log(" getEndpointDowntime(String address) throws UnknownHostException");
        return client.getLongValue("gossiper/downtime/" + address);
    }

    @Override
    public int getCurrentGenerationNumber(String address) throws UnknownHostException {
        log(" getCurrentGenerationNumber(String address) throws UnknownHostException");
        return client.getIntValue("gossiper/generation_number/" + address);
    }

    @Override
    public void unsafeAssassinateEndpoint(String address) throws UnknownHostException {
        log(" unsafeAssassinateEndpoint(String address) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("unsafe", "True");
        client.post("gossiper/assassinate/" + address, queryParams);
    }

    @Override
    public void assassinateEndpoint(String address) throws UnknownHostException {
        log(" assassinateEndpoint(String address) throws UnknownHostException");
        client.post("gossiper/assassinate/" + address, null);
    }

}
