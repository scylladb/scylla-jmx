package com.scylladb.jmx.utils;
/**
 * Copyright 2016 ScyllaDB
 */

import static com.scylladb.jmx.main.Main.client;
import static java.util.logging.Level.SEVERE;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

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

import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MBeanServerBuilder;
import javax.management.MBeanServerDelegate;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.RuntimeOperationsException;

import com.sun.jmx.interceptor.DefaultMBeanServerInterceptor;
import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.sun.jmx.mbeanserver.NamedObject;
import com.sun.jmx.mbeanserver.Repository;
import com.sun.jmx.mbeanserver.Util;

public class APIBuilder extends MBeanServerBuilder {

    private static final Logger logger = Logger.getLogger(APIBuilder.class.getName());

    private static class TableRepository extends Repository {
        private static final Logger logger = Logger.getLogger(TableRepository.class.getName());

        private final Repository wrapped;

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        private final Map<TableMetricParams, DynamicMBean> tableMBeans = new HashMap<>();

        private static boolean isTableMetricName(ObjectName name) {
            return isTableMetricDomain(name.getDomain());
        }

        private static boolean isTableMetricDomain(String domain) {
            return TableMetricParams.TABLE_METRICS_DOMAIN.equals(domain);
        }

        public TableRepository(String defaultDomain, final Repository repository) {
            super(defaultDomain);
            wrapped = repository;
        }

        @Override
        public String getDefaultDomain() {
            return wrapped.getDefaultDomain();
        }

        @Override
        public boolean contains(final ObjectName name) {
            if (!isTableMetricName(name)) {
                return wrapped.contains(name);
            } else {
                lock.readLock().lock();
                try {
                    return tableMBeans.containsKey(new TableMetricParams(name));
                } finally {
                    lock.readLock().unlock();
                }
            }
        }

        @Override
        public String[] getDomains() {
            final String[] domains = wrapped.getDomains();
            if (tableMBeans.isEmpty()) {
                return domains;
            }
            final String[] res = new String[domains.length + 1];
            System.arraycopy(domains, 0, res, 0, domains.length);
            res[domains.length] = TableMetricParams.TABLE_METRICS_DOMAIN;
            return res;
        }

        @Override
        public Integer getCount() {
            lock.readLock().lock();
            try {
                return wrapped.getCount() + tableMBeans.size();
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void addMBean(final DynamicMBean bean, final ObjectName name, final RegistrationContext ctx)
                throws InstanceAlreadyExistsException {
            if (!isTableMetricName(name)) {
                wrapped.addMBean(bean, name, ctx);
            } else {
                final TableMetricParams key = new TableMetricParams(name);
                lock.writeLock().lock();
                try {
                    if (tableMBeans.containsKey(key)) {
                        throw new InstanceAlreadyExistsException(name.toString());
                    }
                    tableMBeans.put(key, bean);
                    if (ctx == null) return;
                    try {
                        ctx.registering();
                    } catch (RuntimeOperationsException x) {
                        throw x;
                    } catch (RuntimeException x) {
                        throw new RuntimeOperationsException(x);
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            }
        }

        @Override
        public void remove(final ObjectName name, final RegistrationContext ctx) throws InstanceNotFoundException {
            if (!isTableMetricName(name)) {
                wrapped.remove(name, ctx);
            } else {
                final TableMetricParams key = new TableMetricParams(name);
                lock.writeLock().lock();
                try {
                    if (tableMBeans.remove(key) == null) {
                        throw new InstanceNotFoundException(name.toString());
                    }

                    if (ctx == null) {
                        return;
                    }
                    try {
                        ctx.unregistered();
                    } catch (Exception x) {
                        logger.log(SEVERE, "Unexpected error.", x);
                    }
                } finally {
                    lock.writeLock().lock();
                }
            }
        }

        @Override
        public DynamicMBean retrieve(final ObjectName name) {
            if (!isTableMetricName(name)) {
                return wrapped.retrieve(name);
            } else {
                lock.readLock().lock();
                try {
                    return tableMBeans.get(new TableMetricParams(name));
                } finally {
                    lock.readLock().unlock();
                }
            }
        }

        private void addAll(final Set<NamedObject> res) {
            for (Map.Entry<TableMetricParams, DynamicMBean> e : tableMBeans.entrySet()) {
                try {
                    res.add(new NamedObject(e.getKey().toName(), e.getValue()));
                } catch (MalformedObjectNameException e1) {
                    // This should never happen
                    logger.log(SEVERE, "Unexpected error.", e1);
                }
            }
        }

        private void addAllMatching(final Set<NamedObject> res,
                final ObjectNamePattern pattern) {
            for (Map.Entry<TableMetricParams, DynamicMBean> e : tableMBeans.entrySet()) {
                try {
                    ObjectName name = e.getKey().toName();
                    if (pattern.matchKeys(name)) {
                        res.add(new NamedObject(name, e.getValue()));
                    }
                } catch (MalformedObjectNameException e1) {
                    // This should never happen
                    logger.log(SEVERE, "Unexpected error.", e1);
                }
            }
        }

        @Override
        public Set<NamedObject> query(final ObjectName pattern, final QueryExp query) {
            Set<NamedObject> res = wrapped.query(pattern, query);
            ObjectName name;
            if (pattern == null ||
                pattern.getCanonicalName().length() == 0 ||
                pattern.equals(ObjectName.WILDCARD)) {
               name = ObjectName.WILDCARD;
            } else {
                name = pattern;
            }

            lock.readLock().lock();
            try {
                // If pattern is not a pattern, retrieve this mbean !
                if (!name.isPattern() && isTableMetricName(name)) {
                    final DynamicMBean bean = tableMBeans.get(new TableMetricParams(name));
                    if (bean != null) {
                        res.add(new NamedObject(name, bean));
                        return res;
                    }
                }

                // All names in all domains
                if (name == ObjectName.WILDCARD) {
                    addAll(res);
                    return res;
                }

                final String canonical_key_property_list_string =
                        name.getCanonicalKeyPropertyListString();

                final boolean allNames =
                        (canonical_key_property_list_string.length()==0);
                final ObjectNamePattern namePattern =
                    (allNames?null:new ObjectNamePattern(name));

                // All names in default domain
                if (name.getDomain().length() == 0) {
                    if (isTableMetricDomain(getDefaultDomain())) {
                        if (allNames) {
                            addAll(res);
                        } else {
                            addAllMatching(res, namePattern);
                        }
                    }
                    return res;
                }

                if (!name.isDomainPattern()) {
                    if (isTableMetricDomain(getDefaultDomain())) {
                        if (allNames) {
                            addAll(res);
                        } else {
                            addAllMatching(res, namePattern);
                        }
                    }
                    return res;
                }

                // Pattern matching in the domain name (*, ?)
                final String dom2Match = name.getDomain();
                if (Util.wildmatch(TableMetricParams.TABLE_METRICS_DOMAIN, dom2Match)) {
                    if (allNames) {
                        addAll(res);
                    } else {
                        addAllMatching(res, namePattern);
                    }
                }
            } finally {
                lock.readLock().unlock();
            }
            return res;
        }
    }

    private final static class ObjectNamePattern {
        private final String[] keys;
        private final String[] values;
        private final String   properties;
        private final boolean  isPropertyListPattern;
        private final boolean  isPropertyValuePattern;

        /**
         * The ObjectName pattern against which ObjectNames are matched.
         **/
        public final ObjectName pattern;

        /**
         * Builds a new ObjectNamePattern object from an ObjectName pattern.
         * @param pattern The ObjectName pattern under examination.
         **/
        public ObjectNamePattern(ObjectName pattern) {
            this(pattern.isPropertyListPattern(),
                 pattern.isPropertyValuePattern(),
                 pattern.getCanonicalKeyPropertyListString(),
                 pattern.getKeyPropertyList(),
                 pattern);
        }

        /**
         * Builds a new ObjectNamePattern object from an ObjectName pattern
         * constituents.
         * @param propertyListPattern pattern.isPropertyListPattern().
         * @param propertyValuePattern pattern.isPropertyValuePattern().
         * @param canonicalProps pattern.getCanonicalKeyPropertyListString().
         * @param keyPropertyList pattern.getKeyPropertyList().
         * @param pattern The ObjectName pattern under examination.
         **/
        ObjectNamePattern(boolean propertyListPattern,
                          boolean propertyValuePattern,
                          String canonicalProps,
                          Map<String,String> keyPropertyList,
                          ObjectName pattern) {
            this.isPropertyListPattern = propertyListPattern;
            this.isPropertyValuePattern = propertyValuePattern;
            this.properties = canonicalProps;
            final int len = keyPropertyList.size();
            this.keys   = new String[len];
            this.values = new String[len];
            int i = 0;
            for (Map.Entry<String,String> entry : keyPropertyList.entrySet()) {
                keys[i]   = entry.getKey();
                values[i] = entry.getValue();
                i++;
            }
            this.pattern = pattern;
        }

        /**
         * Return true if the given ObjectName matches the ObjectName pattern
         * for which this object has been built.
         * WARNING: domain name is not considered here because it is supposed
         *          not to be wildcard when called. PropertyList is also
         *          supposed not to be zero-length.
         * @param name The ObjectName we want to match against the pattern.
         * @return true if <code>name</code> matches the pattern.
         **/
        public boolean matchKeys(ObjectName name) {
            // If key property value pattern but not key property list
            // pattern, then the number of key properties must be equal
            //
            if (isPropertyValuePattern &&
                !isPropertyListPattern &&
                (name.getKeyPropertyList().size() != keys.length)) {
                return false;
            }

            // If key property value pattern or key property list pattern,
            // then every property inside pattern should exist in name
            //
            if (isPropertyValuePattern || isPropertyListPattern) {
                for (int i = keys.length - 1; i >= 0 ; i--) {
                    // Find value in given object name for key at current
                    // index in receiver
                    //
                    String v = name.getKeyProperty(keys[i]);
                    // Did we find a value for this key ?
                    //
                    if (v == null) {
                        return false;
                    }
                    // If this property is ok (same key, same value), go to next
                    //
                    if (isPropertyValuePattern &&
                        pattern.isPropertyValuePattern(keys[i])) {
                        // wildmatch key property values
                        // values[i] is the pattern;
                        // v is the string
                        if (Util.wildmatch(v,values[i])) {
                            continue;
                        } else {
                            return false;
                        }
                    }
                    if (v.equals(values[i])) {
                        continue;
                    }
                    return false;
                }
                return true;
            }

            // If no pattern, then canonical names must be equal
            //
            final String p1 = name.getCanonicalKeyPropertyListString();
            final String p2 = properties;
            return (p1.equals(p2));
        }
    }

    public static class TableMetricParams {
        public static final String TABLE_METRICS_DOMAIN = "org.apache.cassandra.metrics";

        private final ObjectName name;

        public TableMetricParams(ObjectName name) {
            this.name = name;
        }

        public ObjectName toName() throws MalformedObjectNameException {
            return name;
        }

        private static boolean equal(Object a, Object b) {
            return (a == null) ? b == null : a.equals(b);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TableMetricParams)) {
                return false;
            }
            TableMetricParams oo = (TableMetricParams) o;
            return equal(name.getKeyProperty("keyspace"), oo.name.getKeyProperty("keyspace"))
                    && equal(name.getKeyProperty("scope"), oo.name.getKeyProperty("scope"))
                    && equal(name.getKeyProperty("name"), oo.name.getKeyProperty("name"))
                    && equal(name.getKeyProperty("type"), oo.name.getKeyProperty("type"));
        }

        private static int hash(Object o) {
            return o == null ? 0 : o.hashCode();
        }

        private static int safeAdd(int ... nums) {
            long res = 0;
            for (int n : nums) {
                res = (res + n) % Integer.MAX_VALUE;
            }
            return (int)res;
        }

        @Override
        public int hashCode() {
            return safeAdd(hash(name.getKeyProperty("keyspace")),
                    hash(name.getKeyProperty("scope")),
                    hash(name.getKeyProperty("name")),
                    hash(name.getKeyProperty("type")));
        }
    }

    @Override
    public MBeanServer newMBeanServer(String defaultDomain, MBeanServer outer, MBeanServerDelegate delegate) {
        // It is important to set |interceptors| to true while creating the JmxMBeanSearver.
        // It is required for calls to JmxMBeanServer.getMBeanServerInterceptor() to be allowed.
        JmxMBeanServer nested = (JmxMBeanServer) JmxMBeanServer.newMBeanServer(defaultDomain, outer, delegate, true);
        DefaultMBeanServerInterceptor interceptor = (DefaultMBeanServerInterceptor) nested.getMBeanServerInterceptor();
        Field repoField;
        try {
            repoField = DefaultMBeanServerInterceptor.class.getDeclaredField("repository");
        } catch (NoSuchFieldException | SecurityException e) {
            logger.log(SEVERE, "Unexpected error.", e);
            throw new RuntimeException(e);
        }
        repoField.setAccessible(true);
        try {
            final Repository repository = (Repository)repoField.get(interceptor);
            repoField.set(interceptor, new TableRepository(defaultDomain, repository));
        } catch (IllegalArgumentException | IllegalAccessException e) {
            logger.log(SEVERE, "Unexpected error.", e);
            new RuntimeException(e);
        }
        return new APIMBeanServer(client, nested);
    }
}
