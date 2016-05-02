package com.scylladb.jmx.utils;
/**
 * Copyright (C) The MX4J Contributors.
 * All rights reserved.
 *
 * This software is distributed under the terms of the MX4J License version 1.0.
 * See the terms of the MX4J License in the documentation provided with this software.
 */

/**
 * Modified by ScyllaDB
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.management.MBeanInfo;

import mx4j.server.MBeanIntrospector;
import mx4j.server.MBeanMetaData;

public class APIMBeanIntrospector extends MBeanIntrospector {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(APIMBeanIntrospector.class.getName());

    public boolean isMBeanCompliant(MBeanMetaData metadata) {
        Class info = metadata.getMBeanInterface();
        if (info != null) {
            String cn = info.getName();
            if (cn != null) {
                if (cn.endsWith("MXBean")) {
                    return true;
                }
            }
        }
        return super.isMBeanCompliant(metadata);
    }

    public void apiIntrospectStandardMBean(MBeanMetaData metadata) {
        try {
            Class[] cArg = new Class[1];
            cArg[0] = MBeanMetaData.class;
            Method met = MBeanIntrospector.class
                    .getDeclaredMethod("introspectStandardMBean", cArg);
            met.setAccessible(true);
            met.invoke((MBeanIntrospector) this, metadata);
        } catch (NoSuchMethodException | SecurityException
                | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException e) {
            logger.warning("Failed setting mbean info " + e.getMessage());
        }
    }

    public void apiIntrospect(MBeanMetaData metadata) {
        apiIntrospectStandardMBean(metadata);
        Class[] cArg = new Class[1];
        cArg[0] = MBeanMetaData.class;
        try {
            Method met = MBeanIntrospector.class
                    .getDeclaredMethod("createStandardMBeanInfo", cArg);
            met.setAccessible(true);
            Object info = met.invoke((MBeanIntrospector) this, metadata);
            metadata.setMBeanInfo((MBeanInfo) info);
        } catch (IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException
                | InvocationTargetException e) {
            logger.warning("Failed setting mbean info" + e.getMessage());
        }
    }

    public void introspect(MBeanMetaData metadata) {
        Class<?> mx_mbean = null;
        for (Class<?> it : metadata.getMBean().getClass().getInterfaces()) {
            if (it.getName().endsWith("MXBean")) {
                mx_mbean = it;
                break;
            }
        }
        if (mx_mbean != null) {
            metadata.setMBeanInterface(mx_mbean);
            apiIntrospect(metadata);
            return;
        }
        super.introspect(metadata);
    }
}
