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

import java.util.concurrent.TimeUnit;
import com.cloudius.urchin.api.APIClient;
import com.cloudius.urchin.metrics.APIMetrics;
import com.cloudius.urchin.metrics.DefaultNameFactory;
import com.cloudius.urchin.metrics.MetricNameFactory;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics {
    /** Cache capacity in bytes */
    public final Gauge<Long> capacity;
    /** Total number of cache hits */
    public final Meter hits;
    /** Total number of cache requests */
    public final Meter requests;
    /** cache hit rate */
    public final Gauge<Double> hitRate;
    /** Total size of cache, in bytes */
    public final Gauge<Long> size;
    /** Total number of cache entries */
    public final Gauge<Integer> entries;

    private APIClient c = new APIClient();

    /**
     * Create metrics for given cache.
     *
     * @param type
     *            Type of Cache to identify metrics.
     * @param cache
     *            Cache to measure metrics
     */
    public CacheMetrics(String type, String url) {
        MetricNameFactory factory = new DefaultNameFactory("Cache", type);

        capacity = APIMetrics.newGauge(factory.createMetricName("Capacity"),
                new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/cache_service/metrics/" + url
                                + "/capacity");
                    }
                });
        hits = APIMetrics.newMeter("/cache_service/metrics/" + url
                + "/hits", factory.createMetricName("Hits"), "hits",
                TimeUnit.SECONDS);
        requests = APIMetrics.newMeter("/cache_service/metrics/" + url
                + "/requests", factory.createMetricName("Requests"),
                "requests", TimeUnit.SECONDS);
        hitRate = APIMetrics.newGauge(factory.createMetricName("HitRate"),
                new Gauge<Double>() {
                    @Override
                    public Double value() {
                        return c.getDoubleValue("/cache_service/metrics/" + url
                                + "/hit_rate");
                    }
                });
        size = APIMetrics.newGauge(factory.createMetricName("Size"),
                new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/cache_service/metrics/" + url
                                + "/size");
                    }
                });
        entries = APIMetrics.newGauge(factory.createMetricName("Entries"),
                new Gauge<Integer>() {
                    public Integer value() {
                        return c.getIntValue("/cache_service/metrics/" + url
                                + "/entries");
                    }
                });
    }

    // for backward compatibility
    @Deprecated
    public double getRecentHitRate() {
        return 0;
    }
}
