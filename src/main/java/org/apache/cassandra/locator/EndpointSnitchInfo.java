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
package org.apache.cassandra.locator;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.core.MultivaluedMap;

import com.cloudius.urchin.api.APIClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EndpointSnitchInfo implements EndpointSnitchInfoMBean {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(EndpointSnitchInfo.class.getName());

    private APIClient c = new APIClient();

    public void log(String str) {
        logger.info(str);
    }

    private static final EndpointSnitchInfo instance = new EndpointSnitchInfo();

    public static EndpointSnitchInfo getInstance() {
        return instance;
    }

    private EndpointSnitchInfo() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(
                    "org.apache.cassandra.db:type=EndpointSnitchInfo"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Provides the Rack name depending on the respective snitch used, given the
     * host name/ip
     *
     * @param host
     * @throws UnknownHostException
     */
    @Override
    public String getRack(String host) throws UnknownHostException {
        log("getRack(String host) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        if (host == null) {
            host = InetAddress.getLoopbackAddress().getHostAddress();
        }
        queryParams.add("host", host);
        return c.getStringValue("/snitch/rack", queryParams);
    }

    /**
     * Provides the Datacenter name depending on the respective snitch used,
     * given the hostname/ip
     *
     * @param host
     * @throws UnknownHostException
     */
    @Override
    public String getDatacenter(String host) throws UnknownHostException {
        log(" getDatacenter(String host) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        if (host == null) {
            host = InetAddress.getLoopbackAddress().getHostAddress();
        }
        queryParams.add("host", host);
        return c.getStringValue("/snitch/datacenter", queryParams);
    }

    /**
     * Provides the snitch name of the cluster
     *
     * @return Snitch name
     */
    @Override
    public String getSnitchName() {
        log(" getSnitchName()");
        return c.getStringValue("/snitch/name");
    }

}
