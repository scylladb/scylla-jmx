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

import javax.management.MBeanServer;
import javax.management.MBeanServerDelegate;

import mx4j.server.ChainedMBeanServerBuilder;

public class APIBuilder extends ChainedMBeanServerBuilder {
    public APIBuilder() {
        super(new mx4j.server.MX4JMBeanServerBuilder());
    }

    public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer,
            MBeanServerDelegate delegate) {
        APIMBeanServer extern = new APIMBeanServer();
        MBeanServer nested = getMBeanServerBuilder().newMBeanServer(
                defaultDomain, outer == null ? extern : outer, delegate);
        extern.setMBeanServer(nested);
        return extern;
    }
}