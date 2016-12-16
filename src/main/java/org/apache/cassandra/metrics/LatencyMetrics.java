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

import java.util.Arrays;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * Metrics about latencies
 */
public class LatencyMetrics implements Metrics {
    protected final MetricNameFactory[] factories;
    protected final String namePrefix;
    protected final String uri;
    protected final String param;

    /**
     * Create LatencyMetrics with given group, type, and scope. Name prefix for
     * each metric will be empty.
     *
     * @param type
     *            Type name
     * @param scope
     *            Scope
     */
    public LatencyMetrics(String type, String scope, String uri) {
        this(type, "", scope, uri, null);
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
    public LatencyMetrics(String type, String namePrefix, String scope, String uri, String param) {
        this(namePrefix, uri, param, new DefaultNameFactory(type, scope));
    }

    public LatencyMetrics(String namePrefix, String uri, MetricNameFactory... factories) {
        this(namePrefix, uri, null, factories);
    }

    public LatencyMetrics(String namePrefix, String uri, String param, MetricNameFactory... factories) {
        this.factories = factories;
        this.namePrefix = namePrefix;
        this.uri = uri;
        this.param = param;
    }

    protected ObjectName[] names(String suffix) throws MalformedObjectNameException {
        return Arrays.stream(factories).map(f -> {
            try {
                return f.createMetricName(namePrefix + suffix);
            } catch (MalformedObjectNameException e) {
                throw new RuntimeException(e); // dung...
            }
        }).toArray(size -> new ObjectName[size]);
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        String paramName = (param == null) ? "" : "/" + param;
        registry.register(() -> registry.timer(uri + "/moving_average_histogram" + paramName), names("Latency"));
        registry.register(() -> registry.counter(uri + paramName), names("TotalLatency"));
    }
}
