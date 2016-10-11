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

import javax.management.MalformedObjectNameException;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics implements Metrics {

    private final String type;
    private final String url;

    private String compose(String value) {
        return "/cache_service/metrics/" + url + "/" + value;
    }

    /**
     * Create metrics for given cache.
     *
     * @param type
     *            Type of Cache to identify metrics.
     * @param cache
     *            Cache to measure metrics
     */
    public CacheMetrics(String type, final String url) {
        this.type = type;
        this.url = url;
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        MetricNameFactory factory = new DefaultNameFactory("Cache", type);

        registry.register(() -> registry.gauge(compose("capacity")), factory.createMetricName("Capacity"));
        registry.register(() -> registry.meter(compose("hits_moving_avrage")), factory.createMetricName("Hits"));
        registry.register(() -> registry.meter(compose("requests_moving_avrage")),
                factory.createMetricName("Requests"));

        registry.register(() -> registry.gauge(Double.class, compose("hit_rate")), factory.createMetricName("HitRate"));
        registry.register(() -> registry.gauge(compose("size")), factory.createMetricName("Size"));
        registry.register(() -> registry.gauge(Integer.class, compose("entries")), factory.createMetricName("Entries"));
    }
}
