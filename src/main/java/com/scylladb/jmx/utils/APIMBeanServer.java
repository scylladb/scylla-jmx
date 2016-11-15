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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.QueryExp;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.StreamingMetrics;

import mx4j.server.ChainedMBeanServer;
import mx4j.server.MX4JMBeanServer;
import mx4j.util.Utils;

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

    public ObjectName apiNormalizeObjectName(ObjectName name) {
        try {
            Class[] cArg = new Class[1];
            cArg[0] = ObjectName.class;
            Method met = MX4JMBeanServer.class
                    .getDeclaredMethod("normalizeObjectName", cArg);
            met.setAccessible(true);
            return (ObjectName) met.invoke((MX4JMBeanServer) getMBeanServer(),
                    name);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            // TODO Auto-generated catch block
            return null;
        }
    }

    @Override
    public Set<ObjectName> queryNames(ObjectName name, QueryExp query) {
        if (name == null) {
            ColumnFamilyStore.checkRegistration();
            StreamingMetrics.checkRegistration();
            return super.queryNames(name, query);
        }
        if (name.getCanonicalKeyPropertyListString()
                .contains("ColumnFamilies")) {
            ColumnFamilyStore.checkRegistration();
        } else if (name.getCanonicalKeyPropertyListString()
                .contains("Stream")) {
            StreamingMetrics.checkRegistration();
        }
        ObjectName no = apiNormalizeObjectName(name);
        Hashtable patternProps = no.getKeyPropertyList();
        boolean paternFound = false;
        for (Iterator j = patternProps.entrySet().iterator(); j.hasNext();) {
            Map.Entry entry = (Map.Entry) j.next();
            String patternValue = (String) entry.getValue();
            if (patternValue.contains("*")) {
                paternFound = true;
                break;
            }
        }
        if (paternFound) {
            Set<ObjectName> res = new HashSet<ObjectName>();
            for (ObjectName q : (Set<ObjectName>) super.queryNames(null,query)) {
                if (Utils.wildcardMatch(name.getDomain(), q.getDomain())) {
                    Hashtable props = q.getKeyPropertyList();
                    boolean found = true;
                    for (Iterator j = patternProps.entrySet().iterator(); j
                            .hasNext();) {
                        Map.Entry entry = (Map.Entry) j.next();
                        String patternKey = (String) entry.getKey();
                        String patternValue = (String) entry.getValue();
                        if (props.containsKey(patternKey)) {
                            if (!Utils.wildcardMatch(patternValue,
                                    props.get(patternKey).toString())) {
                                found = false;
                                break;
                            }
                        } else {
                            found = false;
                            break;
                        }
                    }
                    if (found) {
                        res.add(q);
                    }
                }
            }
            return res;
        }
        return super.queryNames(name, query);
    }
}