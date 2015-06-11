package com.yammer.metrics.core;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.APICounter;
import com.yammer.metrics.core.APIMeter;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.ThreadPools;
import com.yammer.metrics.core.Histogram.SampleType;

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

public class APIMetricsRegistry extends MetricsRegistry {
    Field field_metrics;
    Field field_clock;
    Field field_thread_pool;
    
    public APIMetricsRegistry() {
        try {
            field_metrics = MetricsRegistry.class.getDeclaredField("metrics");
            field_metrics.setAccessible(true);
            field_clock = MetricsRegistry.class.getDeclaredField("clock");
            field_clock.setAccessible(true);
            field_thread_pool = MetricsRegistry.class.getDeclaredField("threadPools");
            field_thread_pool.setAccessible(true);
        } catch (NoSuchFieldException | SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public ThreadPools getThreadPools() {
        try {
            return (ThreadPools)field_thread_pool.get(this);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();            
        }
        return null;
    }
    
    public Clock getClock() {
        try {
            return (Clock)field_clock.get(this);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();            
        }
        return null;
    }
    
    @SuppressWarnings("unchecked")
    public ConcurrentMap<MetricName, Metric> get_metrics() {
        try {
            return (ConcurrentMap<MetricName, Metric>)field_metrics.get(this);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();            
        }
        return null;
    }
    /**
     * Creates a new {@link Counter} and registers it under the given class and name.
     *
     * @param klass the class which owns the metric
     * @param name  the name of the metric
     * @return a new {@link Counter}
     */
    public Counter newCounter(String url, Class<?> klass,
                              String name) {
        return newCounter(url, klass, name, null);
    }

    /**
     * Creates a new {@link Counter} and registers it under the given class and name.
     *
     * @param klass the class which owns the metric
     * @param name  the name of the metric
     * @param scope the scope of the metric
     * @return a new {@link Counter}
     */
    public Counter newCounter(String url, Class<?> klass,
                              String name,
                              String scope) {
        return newCounter(url, createName(klass, name, scope));
    }
    /**
     * Creates a new {@link Counter} and registers it under the given metric name.
     *
     * @param metricName the name of the metric
     * @return a new {@link Counter}
     */
    public Counter newCounter(String url, MetricName metricName) {
        return getOrAdd(metricName, new APICounter(url));
    }
    
    /**
     * Creates a new {@link Meter} and registers it under the given class and name.
     *
     * @param klass     the class which owns the metric
     * @param name      the name of the metric
     * @param eventType the plural name of the type of events the meter is measuring (e.g., {@code
     *                  "requests"})
     * @param unit      the rate unit of the new meter
     * @return a new {@link Meter}
     */
    public Meter newMeter(String url, Class<?> klass,
                          String name,
                          String eventType,
                          TimeUnit unit) {
        return newMeter(url, klass, name, null, eventType, unit);
    }

    /**
     * Creates a new {@link Meter} and registers it under the given class, name, and scope.
     *
     * @param klass     the class which owns the metric
     * @param name      the name of the metric
     * @param scope     the scope of the metric
     * @param eventType the plural name of the type of events the meter is measuring (e.g., {@code
     *                  "requests"})
     * @param unit      the rate unit of the new meter
     * @return a new {@link Meter}
     */
    public Meter newMeter(String url,
                          Class<?> klass,
                          String name,
                          String scope,
                          String eventType,
                          TimeUnit unit) {
        return newMeter(url, createName(klass, name, scope), eventType, unit);
    }

    private ScheduledExecutorService newMeterTickThreadPool() {
        return getThreadPools().newScheduledThreadPool(2, "meter-tick");
    }
    /**
     * Creates a new {@link Meter} and registers it under the given metric name.
     *
     * @param metricName the name of the metric
     * @param eventType  the plural name of the type of events the meter is measuring (e.g., {@code
     *                   "requests"})
     * @param unit       the rate unit of the new meter
     * @return a new {@link Meter}
     */
    public Meter newMeter(String url, MetricName metricName,
                          String eventType,
                          TimeUnit unit) {
        final Metric existingMetric = get_metrics().get(metricName);
        if (existingMetric != null) {
            return (Meter) existingMetric;
        }
        return getOrAdd(metricName, new APIMeter(url, newMeterTickThreadPool(), eventType, unit, getClock()));
    }
    
    /**
     * Creates a new {@link Histogram} and registers it under the given class and name.
     *
     * @param klass  the class which owns the metric
     * @param name   the name of the metric
     * @param biased whether or not the histogram should be biased
     * @return a new {@link Histogram}
     */
    public Histogram newHistogram(String url, Class<?> klass,
                                  String name,
                                  boolean biased) {
        return newHistogram(url, klass, name, null, biased);
    }

    /**
     * Creates a new {@link Histogram} and registers it under the given class, name, and scope.
     *
     * @param klass  the class which owns the metric
     * @param name   the name of the metric
     * @param scope  the scope of the metric
     * @param biased whether or not the histogram should be biased
     * @return a new {@link Histogram}
     */
    public Histogram newHistogram(String url, Class<?> klass,
                                  String name,
                                  String scope,
                                  boolean biased) {
        return newHistogram(url, createName(klass, name, scope), biased);
    }

    /**
     * Creates a new non-biased {@link Histogram} and registers it under the given class and name.
     *
     * @param klass the class which owns the metric
     * @param name  the name of the metric
     * @return a new {@link Histogram}
     */
    public Histogram newHistogram(String url, Class<?> klass,
                                  String name) {
        return newHistogram(url, klass, name, false);
    }

    /**
     * Creates a new non-biased {@link Histogram} and registers it under the given class, name, and
     * scope.
     *
     * @param klass the class which owns the metric
     * @param name  the name of the metric
     * @param scope the scope of the metric
     * @return a new {@link Histogram}
     */
    public Histogram newHistogram(String url, Class<?> klass,
                                  String name,
                                  String scope) {
        return newHistogram(url, klass, name, scope, false);
    }

    /**
     * Creates a new {@link Histogram} and registers it under the given metric name.
     *
     * @param metricName the name of the metric
     * @param biased     whether or not the histogram should be biased
     * @return a new {@link Histogram}
     */
    public Histogram newHistogram(String url, MetricName metricName,
                                  boolean biased) {
        return getOrAdd(metricName,
                        new APIHistogram(url, biased ? SampleType.BIASED : SampleType.UNIFORM));
    }
}
