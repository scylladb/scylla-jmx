/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
package org.apache.cassandra.metrics;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.scylladb.jmx.metrics.APIMetrics;
import com.scylladb.jmx.metrics.DefaultNameFactory;
import com.scylladb.jmx.metrics.MetricNameFactory;
import com.scylladb.jmx.utils.RecentEstimatedHistogram;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;

/**
 * Metrics about latencies
 */
public class LatencyMetrics {
    /** Latency */
    public final Timer latency;
    /** Total latency in micro sec */
    public final Counter totalLatency;

    /** parent metrics to replicate any updates to **/
    private List<LatencyMetrics> parents = Lists.newArrayList();

    protected final MetricNameFactory factory;
    protected final String namePrefix;

    @Deprecated public EstimatedHistogramWrapper totalLatencyHistogram;
    /*
     * It should not be called directly, use the getRecentLatencyHistogram
     */
    @Deprecated protected final RecentEstimatedHistogram recentLatencyHistogram = new RecentEstimatedHistogram();

    protected long lastLatency;
    protected long lastOpCount;

    /**
     * Create LatencyMetrics with given group, type, and scope. Name prefix for
     * each metric will be empty.
     *
     * @param type
     *            Type name
     * @param scope
     *            Scope
     */
    public LatencyMetrics(String url, String type, String scope) {
        this(url, type, "", scope);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each
     * metric name, and scope.
     *
     * @param type
     *            Type name
     * @param namePrefix
     *            Prefix to append to each metric name
     * @param scope
     *            Scope of metrics
     */
    public LatencyMetrics(String url, String type, String namePrefix,
            String scope) {
        this(url, new DefaultNameFactory(type, scope), namePrefix);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each
     * metric name, and scope.
     *
     * @param factory
     *            MetricName factory to use
     * @param namePrefix
     *            Prefix to append to each metric name
     */
    public LatencyMetrics(String url, MetricNameFactory factory,
            String namePrefix) {
        this(url, null, factory, namePrefix);
    }

    public LatencyMetrics(String url, String paramName,
            MetricNameFactory factory, String namePrefix) {
        this.factory = factory;
        this.namePrefix = namePrefix;

        paramName = (paramName == null)? "" : "/" + paramName;
        latency = APIMetrics.newTimer(url + "/moving_average_histogram" + paramName,
                factory.createMetricName(namePrefix + "Latency"),
                TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        totalLatency = APIMetrics.newCounter(url +  paramName,
                factory.createMetricName(namePrefix + "TotalLatency"));
        totalLatencyHistogram = new EstimatedHistogramWrapper(url + "/estimated_histogram" + paramName);
    }

    /**
     * Create LatencyMetrics with given group, type, prefix to append to each
     * metric name, and scope. Any updates to this will also run on parent
     *
     * @param factory
     *            MetricName factory to use
     * @param namePrefix
     *            Prefix to append to each metric name
     * @param parents
     *            any amount of parents to replicate updates to
     */
    public LatencyMetrics(String url, MetricNameFactory factory,
            String namePrefix, LatencyMetrics... parents) {
        this(url, factory, namePrefix);
        this.parents.addAll(ImmutableList.copyOf(parents));
    }

    /** takes nanoseconds **/
    public void addNano(long nanos) {
        // the object is only updated from the API
    }

    public void release() {
        APIMetrics.defaultRegistry()
                .removeMetric(factory.createMetricName(namePrefix + "Latency"));
        APIMetrics.defaultRegistry().removeMetric(
                factory.createMetricName(namePrefix + "TotalLatency"));
    }

    @Deprecated
    public synchronized double getRecentLatency() {
        long ops = latency.count();
        long n = totalLatency.count();
        if (ops == lastOpCount)
            return 0;
        try {
            return ((double) n - lastLatency) / (ops - lastOpCount);
        } finally {
            lastLatency = n;
            lastOpCount = ops;
        }
    }

    public long[] getRecentLatencyHistogram() {
        return recentLatencyHistogram.getBuckets(totalLatencyHistogram.getBuckets(false));
    }
}
