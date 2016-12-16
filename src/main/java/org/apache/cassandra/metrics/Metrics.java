package org.apache.cassandra.metrics;

import java.util.function.Function;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;

/**
 * Action interface for any type that encapsulates n metrics.
 * 
 * @author calle
 *
 */
public interface Metrics {
    /**
     * Implementors should issue
     * {@link MetricsRegistry#register(java.util.function.Supplier, javax.management.ObjectName...)}
     * for every {@link Metrics} they generate. This method is called in both
     * bind (create) and unbind (remove) phase, so an appropriate use of
     * {@link Function} binding is advisable.
     * 
     * @param registry
     * @throws MalformedObjectNameException
     */
    void register(MetricsRegistry registry) throws MalformedObjectNameException;

    /**
     * Same as {{@link #register(MetricsRegistry)}, but for {@link Metric}s that
     * are "global" (i.e. static - not bound to an individual bean instance.
     * This method is called whenever the first encapsulating MBean is
     * added/removed from a {@link MBeanServer}.
     * 
     * @param registry
     * @throws MalformedObjectNameException
     */
    default void registerGlobals(MetricsRegistry registry) throws MalformedObjectNameException {
    }
}
