package com.scylladb.jmx.metrics;

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.APIMetricsRegistry;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.JmxReporter;

public class APIMetrics {
    private static final APIMetricsRegistry DEFAULT_REGISTRY = new APIMetricsRegistry();
    private static final Thread SHUTDOWN_HOOK = new Thread() {
        public void run() {
            JmxReporter.shutdownDefault();
        }
    };

    static {
        JmxReporter.startDefault(DEFAULT_REGISTRY);
        Runtime.getRuntime().addShutdownHook(SHUTDOWN_HOOK);
    }

    private APIMetrics() { /* unused */
    }

    /**
     * Given a new {@link com.yammer.metrics.core.Gauge}, registers it under the
     * given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param metric
     *            the metric
     * @param <T>
     *            the type of the value returned by the metric
     * @return {@code metric}
     */
    public static <T> Gauge<T> newGauge(Class<?> klass, String name,
            Gauge<T> metric) {
        return DEFAULT_REGISTRY.newGauge(klass, name, metric);
    }

    /**
     * Given a new {@link com.yammer.metrics.core.Gauge}, registers it under the
     * given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @param metric
     *            the metric
     * @param <T>
     *            the type of the value returned by the metric
     * @return {@code metric}
     */
    public static <T> Gauge<T> newGauge(Class<?> klass, String name,
            String scope, Gauge<T> metric) {
        return DEFAULT_REGISTRY.newGauge(klass, name, scope, metric);
    }

    /**
     * Given a new {@link com.yammer.metrics.core.Gauge}, registers it under the
     * given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @param metric
     *            the metric
     * @param <T>
     *            the type of the value returned by the metric
     * @return {@code metric}
     */
    public static <T> Gauge<T> newGauge(MetricName metricName, Gauge<T> metric) {
        return DEFAULT_REGISTRY.newGauge(metricName, metric);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Counter} and registers it
     * under the given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @return a new {@link com.yammer.metrics.core.Counter}
     */
    public static Counter newCounter(String url, Class<?> klass, String name) {
        return DEFAULT_REGISTRY.newCounter(url, klass, name);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Counter} and registers it
     * under the given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @return a new {@link com.yammer.metrics.core.Counter}
     */
    public static Counter newCounter(String url, Class<?> klass, String name,
            String scope) {
        return DEFAULT_REGISTRY.newCounter(url, klass, name, scope);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Counter} and registers it
     * under the given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @return a new {@link com.yammer.metrics.core.Counter}
     */
    public static Counter newCounter(String url, MetricName metricName) {
        return DEFAULT_REGISTRY.newCounter(url, metricName);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Histogram} and registers it
     * under the given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param biased
     *            whether or not the histogram should be biased
     * @return a new {@link com.yammer.metrics.core.Histogram}
     */
    public static Histogram newHistogram(String url, Class<?> klass,
            String name, boolean biased) {
        return DEFAULT_REGISTRY.newHistogram(url, klass, name, biased);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Histogram} and registers it
     * under the given class, name, and scope.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @param biased
     *            whether or not the histogram should be biased
     * @return a new {@link com.yammer.metrics.core.Histogram}
     */
    public static Histogram newHistogram(String url, Class<?> klass,
            String name, String scope, boolean biased) {
        return DEFAULT_REGISTRY.newHistogram(url, klass, name, scope, biased);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Histogram} and registers it
     * under the given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @param biased
     *            whether or not the histogram should be biased
     * @return a new {@link com.yammer.metrics.core.Histogram}
     */
    public static Histogram newHistogram(String url, MetricName metricName,
            boolean biased) {
        return DEFAULT_REGISTRY.newHistogram(url, metricName, biased);
    }

    /**
     * Creates a new non-biased {@link com.yammer.metrics.core.Histogram} and
     * registers it under the given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @return a new {@link com.yammer.metrics.core.Histogram}
     */
    public static Histogram newHistogram(String url, Class<?> klass, String name) {
        return DEFAULT_REGISTRY.newHistogram(url, klass, name);
    }

    /**
     * Creates a new non-biased {@link com.yammer.metrics.core.Histogram} and
     * registers it under the given class, name, and scope.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @return a new {@link com.yammer.metrics.core.Histogram}
     */
    public static Histogram newHistogram(String url, Class<?> klass,
            String name, String scope) {
        return DEFAULT_REGISTRY.newHistogram(url, klass, name, scope);
    }

    /**
     * Creates a new non-biased {@link com.yammer.metrics.core.Histogram} and
     * registers it under the given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @return a new {@link com.yammer.metrics.core.Histogram}
     */
    public static Histogram newHistogram(String url, MetricName metricName) {
        return newHistogram(url, metricName, false);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Meter} and registers it
     * under the given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param eventType
     *            the plural name of the type of events the meter is measuring
     *            (e.g., {@code "requests"})
     * @param unit
     *            the rate unit of the new meter
     * @return a new {@link com.yammer.metrics.core.Meter}
     */
    public static Meter newMeter(String url, Class<?> klass, String name,
            String eventType, TimeUnit unit) {
        return DEFAULT_REGISTRY.newMeter(url, klass, name, eventType, unit);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Meter} and registers it
     * under the given class, name, and scope.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @param eventType
     *            the plural name of the type of events the meter is measuring
     *            (e.g., {@code "requests"})
     * @param unit
     *            the rate unit of the new meter
     * @return a new {@link com.yammer.metrics.core.Meter}
     */
    public static Meter newMeter(String url, Class<?> klass, String name,
            String scope, String eventType, TimeUnit unit) {
        return DEFAULT_REGISTRY.newMeter(url, klass, name, scope, eventType,
                unit);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Meter} and registers it
     * under the given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @param eventType
     *            the plural name of the type of events the meter is measuring
     *            (e.g., {@code "requests"})
     * @param unit
     *            the rate unit of the new meter
     * @return a new {@link com.yammer.metrics.core.Meter}
     */
    public static Meter newMeter(String url, MetricName metricName,
            String eventType, TimeUnit unit) {
        return DEFAULT_REGISTRY.newMeter(url, metricName, eventType, unit);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.Meter} and registers it
     * under the given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @param eventType
     *            the plural name of the type of events the meter is measuring
     *            (e.g., {@code "requests"})
     * @param unit
     *            the rate unit of the new meter
     * @return a new {@link com.yammer.metrics.core.Meter}
     */
    public static Meter newSettableMeter(MetricName metricName,
            String eventType, TimeUnit unit) {
        return DEFAULT_REGISTRY.newSettableMeter(metricName, eventType, unit);
    }
    /**
     * Creates a new {@link com.yammer.metrics.core.APITimer} and registers it
     * under the given class and name.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param durationUnit
     *            the duration scale unit of the new timer
     * @param rateUnit
     *            the rate scale unit of the new timer
     * @return a new {@link com.yammer.metrics.core.APITimer}
     */
    public static Timer newTimer(String url, Class<?> klass, String name,
            TimeUnit durationUnit, TimeUnit rateUnit) {
        return DEFAULT_REGISTRY.newTimer(url, klass, name, durationUnit, rateUnit);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.APITimer} and registers it
     * under the given class and name, measuring elapsed time in milliseconds
     * and invocations per second.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @return a new {@link com.yammer.metrics.core.APITimer}
     */
    public static Timer newTimer(String url, Class<?> klass, String name) {
        return DEFAULT_REGISTRY.newTimer(url, klass, name);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.APITimer} and registers it
     * under the given class, name, and scope.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @param durationUnit
     *            the duration scale unit of the new timer
     * @param rateUnit
     *            the rate scale unit of the new timer
     * @return a new {@link com.yammer.metrics.core.APITimer}
     */
    public static Timer newTimer(String url, Class<?> klass, String name, String scope,
            TimeUnit durationUnit, TimeUnit rateUnit) {
        return DEFAULT_REGISTRY.newTimer(url, klass, name, scope, durationUnit,
                rateUnit);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.APITimer} and registers it
     * under the given class, name, and scope, measuring elapsed time in
     * milliseconds and invocations per second.
     *
     * @param klass
     *            the class which owns the metric
     * @param name
     *            the name of the metric
     * @param scope
     *            the scope of the metric
     * @return a new {@link com.yammer.metrics.core.APITimer}
     */
    public static Timer newTimer(String url, Class<?> klass, String name, String scope) {
        return DEFAULT_REGISTRY.newTimer(url, klass, name, scope);
    }

    /**
     * Creates a new {@link com.yammer.metrics.core.APITimer} and registers it
     * under the given metric name.
     *
     * @param metricName
     *            the name of the metric
     * @param durationUnit
     *            the duration scale unit of the new timer
     * @param rateUnit
     *            the rate scale unit of the new timer
     * @return a new {@link com.yammer.metrics.core.APITimer}
     */
    public static Timer newTimer(String url, MetricName metricName, TimeUnit durationUnit,
            TimeUnit rateUnit) {
        return DEFAULT_REGISTRY.newTimer(url, metricName, durationUnit, rateUnit);
    }

    /**
     * Returns the (static) default registry.
     *
     * @return the metrics registry
     */
    public static APIMetricsRegistry defaultRegistry() {
        return DEFAULT_REGISTRY;
    }

    /**
     * Shuts down all thread pools for the default registry.
     */
    public static void shutdown() {
        DEFAULT_REGISTRY.shutdown();
        JmxReporter.shutdownDefault();
        Runtime.getRuntime().removeShutdownHook(SHUTDOWN_HOOK);
    }

}
