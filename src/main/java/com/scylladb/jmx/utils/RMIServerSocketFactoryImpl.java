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
 * Copyright 2016 ScyllaDB
 *
 * Modified by ScyllaDB
 */

package com.scylladb.jmx.utils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import javax.net.ServerSocketFactory;

public class RMIServerSocketFactoryImpl implements RMIServerSocketFactory {
    public static JMXConnectorServer jmxServer = null;

    public static void maybeInitJmx() {
        System.setProperty("javax.management.builder.initial", "com.scylladb.jmx.utils.APIBuilder");
        System.setProperty("mx4j.strict.mbean.interface", "no");

        String jmxPort = System
                .getProperty("com.sun.management.jmxremote.port");

        if (jmxPort == null) {
            System.out.println(
                    "JMX is not enabled to receive remote connections.");

            jmxPort = System.getProperty("cassandra.jmx.local.port", "7199");
            String address = System.getProperty("jmx.address", "localhost");
            if (address.equals("localhost")) {
                System.setProperty("java.rmi.server.hostname",
                        InetAddress.getLoopbackAddress().getHostAddress());
            } else {
                try {
                    System.setProperty("java.rmi.server.hostname",
                            InetAddress.getByName(address).getHostAddress());
                } catch (UnknownHostException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

            }
            try {
                RMIServerSocketFactory serverFactory = new RMIServerSocketFactoryImpl();
                LocateRegistry.createRegistry(Integer.valueOf(jmxPort), null,
                        serverFactory);

                StringBuffer url = new StringBuffer();
                url.append("service:jmx:");
                url.append("rmi://").append(address).append("/jndi/");
                url.append("rmi://").append(address).append(":").append(jmxPort)
                        .append("/jmxrmi");
                System.out.println(url);
                Map env = new HashMap();
                env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE,
                        serverFactory);

                jmxServer = new RMIConnectorServer(
                        new JMXServiceURL(url.toString()), env,
                        ManagementFactory.getPlatformMBeanServer());

                jmxServer.start();
            } catch (IOException e) {
                System.out.println(
                        "Error starting local jmx server: " + e.toString());
            }

        } else {
            System.out.println(
                    "JMX is enabled to receive remote connections on port: "
                            + jmxPort);
        }
    }

    public ServerSocket createServerSocket(final int pPort) throws IOException {
        return ServerSocketFactory.getDefault().createServerSocket(pPort, 0,
                InetAddress.getLoopbackAddress());
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        return obj.getClass().equals(getClass());
    }

    public int hashCode() {
        return RMIServerSocketFactoryImpl.class.hashCode();
    }

}
