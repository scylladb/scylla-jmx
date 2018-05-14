package com.scylladb.jmx.metrics;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.metrics.Metrics;
import org.apache.cassandra.metrics.MetricsRegistry;

import com.scylladb.jmx.api.APIClient;
import com.sun.jmx.mbeanserver.JmxMBeanServer;

/**
 * Base type for MBeans containing {@link Metrics}. 
 * 
 * @author calle
 *
 */
public abstract class MetricsMBean extends APIMBean {
    private static final Map<JmxMBeanServer, Map<String, Integer>> registered = new HashMap<>();
    private static final Object registrationLock = new Object();

    private final Collection<Metrics> metrics;

    public MetricsMBean(APIClient client, Metrics... metrics) {
        this(null, client, metrics);
    }

    public MetricsMBean(String mbeanName, APIClient client, Metrics... metrics) {
        this(mbeanName, client, asList(metrics));
    }

    public MetricsMBean(String mbeanName, APIClient client, Collection<Metrics> metrics) {
        super(mbeanName, client);
        this.metrics = metrics;
    }

    protected Predicate<ObjectName> getTypePredicate() {
        String domain = name.getDomain();
        String type = name.getKeyProperty("type");
        return n -> {
            return domain.equals(n.getDomain()) && type.equals(n.getKeyProperty("type"));
        };
    }

    // Has to be called with registrationLock hold
    private static boolean shouldRegisterGlobals(JmxMBeanServer server, String domainAndType, boolean reversed) {
        Map<String, Integer> serverMap = registered.get(server);
        if (serverMap == null) {
            assert !reversed;
            serverMap = new HashMap<>();
            serverMap.put(domainAndType, 1);
            registered.put(server, serverMap);
            return true;
        }
        Integer count = serverMap.get(domainAndType);
        if (count == null) {
            assert !reversed;
            serverMap.put(domainAndType, 1);
            return true;
        }
        if (reversed) {
            --count;
            if (count == 0) {
                serverMap.remove(domainAndType);
                if (serverMap.isEmpty()) {
                    registered.remove(server);
                }
                return true;
            }
            serverMap.put(domainAndType, count);
            return false;
        } else {
            serverMap.put(domainAndType, count + 1);
        }
        return false;
    }

    private void register(MetricsRegistry registry, JmxMBeanServer server, boolean reversed) throws MalformedObjectNameException {
        // Check if we're the first/last of our type bound/removed. 
        synchronized (registrationLock) {
            boolean registerGlobals = shouldRegisterGlobals(server, name.getDomain() + ":" + name.getKeyProperty("type"), reversed);
            if (registerGlobals) {
                for (Metrics m : metrics) {
                    m.registerGlobals(registry);
                }
            }
        }
        for (Metrics m : metrics) {
            m.register(registry);
        }
    }

    @Override
    public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
        // Get name etc. 
        name = super.preRegister(server, name);
        // Register all metrics in server
        register(new MetricsRegistry(client, (JmxMBeanServer) server), (JmxMBeanServer) server, false);
        return name;
    }

    @Override
    public void postDeregister() {
        // We're officially unbound. Remove all metrics we added.
        try {
            register(new MetricsRegistry(client, server) {
                // Unbind instead of bind. Yes.
                @Override
                public void register(Supplier<MetricMBean> s, ObjectName... objectNames) {
                    for (ObjectName name : objectNames) {
                        try {
                            server.getMBeanServerInterceptor().unregisterMBean(name);
                        } catch (MBeanRegistrationException | InstanceNotFoundException e) {
                        }
                    }
                }
            }, server, true);
        } catch (MalformedObjectNameException e) {
            // TODO : log?
        }
        super.postDeregister();
    }
}
