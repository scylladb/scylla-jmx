package com.scylladb.jmx.utils;
/**
 * Copyright 2016 ScyllaDB
 */

import static com.scylladb.jmx.main.Main.client;

/*
* This file is part of Scylla.
*
* Scylla is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* Scylla is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
*/

import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;

import com.sun.jmx.mbeanserver.JmxMBeanServer;

public class APIBuilder extends MBeanServerBuilder {
    @Override
    public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
        // It is important to set |interceptors| to true while creating the JmxMBeanSearver.
        // It is required for calls to JmxMBeanServer.getMBeanServerInterceptor() to be allowed.
        JmxMBeanServer nested = (JmxMBeanServer) JmxMBeanServer.newMBeanServer(defaultDomain, outer, delegate, true);
        return new APIMBeanServer(client, nested);
    }
}