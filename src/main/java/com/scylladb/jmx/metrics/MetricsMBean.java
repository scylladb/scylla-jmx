package com.scylladb.jmx.metrics;

import static java.util.Arrays.asList;

import java.util.Collection;
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

/**
 * Base type for MBeans containing {@link Metrics}. 
 * 
 * @author calle
 *
 */
public abstract class MetricsMBean extends APIMBean {
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

    private void register(MetricsRegistry registry, MBeanServer server) throws MalformedObjectNameException {
        // Check if we're the first/last of our type bound/removed. 
        boolean empty = queryNames(server, getTypePredicate()).isEmpty();
        for (Metrics m : metrics) {
            if (empty) {
                m.registerGlobals(registry);
            }
            m.register(registry);
        }
    }

    @Override
    public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
        // Get name etc. 
        name = super.preRegister(server, name);
        // Register all metrics in server
        register(new MetricsRegistry(client, server), server);
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
                            server.unregisterMBean(name);
                        } catch (MBeanRegistrationException | InstanceNotFoundException e) {
                        }
                    }
                }
            }, server);
        } catch (MalformedObjectNameException e) {
            // TODO : log?
        }
        super.postDeregister();
    }
}
