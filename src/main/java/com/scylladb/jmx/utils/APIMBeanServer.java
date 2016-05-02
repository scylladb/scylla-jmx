package com.scylladb.jmx.utils;

/**
 * Copyright 2016 ScyllaDB
 */
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
import java.lang.reflect.Field;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.StreamingMetrics;

import mx4j.server.ChainedMBeanServer;

public class APIMBeanServer extends ChainedMBeanServer {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(APIMBeanServer.class.getName());

    public static void log(String str) {
        logger.finest(str);
    }

    public void setMBeanServer(MBeanServer server) {
        if (server != null) {
            try {
                Field f = server.getClass().getDeclaredField("introspector");
                f.setAccessible(true);
                f.set(server, new APIMBeanIntrospector());
            } catch (Exception e) {
                logger.warning(
                        "Failed setting new interceptor" + e.getMessage());
            }
        }
        super.setMBeanServer(server);
    }

    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
        if (name.getCanonicalKeyPropertyListString()
                .contains("ColumnFamilies")) {
            ColumnFamilyStore.checkRegistration();
        } else if (name.getCanonicalKeyPropertyListString()
                .contains("Stream")) {
            StreamingMetrics.checkRegistration();
        }
        return super.queryNames(name, query);
    }
}