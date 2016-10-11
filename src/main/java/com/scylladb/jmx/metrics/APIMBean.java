package com.scylladb.jmx.metrics;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.management.BadAttributeValueExpException;
import javax.management.BadBinaryOpValueExpException;
import javax.management.BadStringOperationException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.InvalidApplicationException;
import javax.management.MBeanRegistration;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.QueryExp;

import com.scylladb.jmx.api.APIClient;

/**
 * Base type for MBeans in scylla-jmx. Wraps auto naming and {@link APIClient}
 * holding.
 * 
 * @author calle
 *
 */
public class APIMBean implements MBeanRegistration {
    protected final APIClient client;
    protected final String mbeanName;

    public APIMBean(APIClient client) {
        this(null, client);
    }

    public APIMBean(String mbeanName, APIClient client) {
        this.mbeanName = mbeanName;
        this.client = client;
    }

    /**
     * Helper method to add/remove dynamically created MBeans from a server
     * instance.
     * 
     * @param server
     *            The {@link MBeanServer} to check
     * @param all
     *            All {@link ObjectName}s that should be bound
     * @param predicate
     *            {@link QueryExp} predicate to filter relevant object names.
     * @param generator
     *            {@link Function} to create a new MBean instance for a given
     *            {@link ObjectName}
     * 
     * @return
     * @throws MalformedObjectNameException
     */
    public static boolean checkRegistration(MBeanServer server, Set<ObjectName> all,
            final Predicate<ObjectName> predicate, Function<ObjectName, Object> generator)
            throws MalformedObjectNameException {
        Set<ObjectName> registered = queryNames(server, predicate);
        for (ObjectName name : registered) {
            if (!all.contains(name)) {
                try {
                    server.unregisterMBean(name);
                } catch (MBeanRegistrationException | InstanceNotFoundException e) {
                }
            }
        }

        int added = 0;
        for (ObjectName name : all) {
            if (!registered.contains(name)) {
                try {
                    server.registerMBean(generator.apply(name), name);
                    added++;
                } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
                }
            }
        }
        return added > 0;
    }

    /**
     * Helper method to query {@link ObjectName}s from an {@link MBeanServer}
     * based on {@link Predicate}
     * 
     * @param server
     * @param predicate
     * @return
     */
    public static Set<ObjectName> queryNames(MBeanServer server, final Predicate<ObjectName> predicate) {
        @SuppressWarnings("serial")
        Set<ObjectName> registered = server.queryNames(null, new QueryExp() {
            @Override
            public void setMBeanServer(MBeanServer s) {
            }

            @Override
            public boolean apply(ObjectName name) throws BadStringOperationException, BadBinaryOpValueExpException,
                    BadAttributeValueExpException, InvalidApplicationException {
                return predicate.test(name);
            }
        });
        return registered;
    }

    MBeanServer server;
    ObjectName name;

    protected final ObjectName getBoundName() {
        return name;
    }

    /**
     * Figure out an {@link ObjectName} for this object based on either
     * contructor parameter, static field, or just package/class name.
     * 
     * @return
     * @throws MalformedObjectNameException
     */
    protected ObjectName generateName() throws MalformedObjectNameException {
        String mbeanName = this.mbeanName;
        if (mbeanName == null) {
            Field f;
            try {
                f = getClass().getDeclaredField("MBEAN_NAME");
                f.setAccessible(true);
                mbeanName = (String) f.get(null);
            } catch (Throwable t) {
            }
        }
        if (mbeanName == null) {
            String name = getClass().getName();
            int i = name.lastIndexOf('.');
            mbeanName = name.substring(0, i) + ":type=" + name.substring(i + 1);
        }
        return new ObjectName(mbeanName);
    }

    /**
     * Keeps track of bound server and optionally generates an
     * {@link ObjectName} for this instance.
     */
    @Override
    public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
        if (this.server != null) {
            throw new IllegalStateException("Can only exist in a single MBeanServer");
        }
        this.server = server;
        if (name == null) {
            name = generateName();
        }
        this.name = name;

        return name;
    }

    @Override
    public void postRegister(Boolean registrationDone) {
    }

    @Override
    public void preDeregister() throws Exception {
    }

    @Override
    public void postDeregister() {
        assert server != null;
        assert name != null;
        this.server = null;
        this.name = null;
    }
}
