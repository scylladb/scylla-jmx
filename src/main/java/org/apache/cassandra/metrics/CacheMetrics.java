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
import java.util.concurrent.atomic.AtomicLong;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMetrics;
import com.scylladb.jmx.metrics.DefaultNameFactory;
import com.scylladb.jmx.metrics.MetricNameFactory;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.APIMeter;

/**
 * Metrics for {@code ICache}.
 */
public class CacheMetrics {
    /** Cache capacity in bytes */
    public final Gauge<Long> capacity;
    /** Total number of cache hits */
    public final APIMeter hits;
    /** Total number of cache requests */
    public final APIMeter requests;
    /** cache hit rate */
    public final Gauge<Double> hitRate;
    /** Total size of cache, in bytes */
    public final Gauge<Long> size;
    /** Total number of cache entries */
    public final Gauge<Integer> entries;

    private final AtomicLong lastRequests = new AtomicLong(0);
    private final AtomicLong lastHits = new AtomicLong(0);

    private APIClient c = new APIClient();

    private String getURL(String url, String value) {
        if (url == null || value == null) {
            return null;
        }
        return "/cache_service/metrics/" + url + value;
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
        MetricNameFactory factory = new DefaultNameFactory("Cache", type);


        capacity = APIMetrics.newGauge(factory.createMetricName("Capacity"),
                new Gauge<Long>() {
                    String u = getURL(url, "/capacity");
                    public Long value() {
                        if (u == null) {
                            return 0L;
                        }
                        return c.getLongValue(u);
                    }
                });
        hits = APIMetrics.newMeter(getURL(url, "/hits_moving_avrage"), factory.createMetricName("Hits"), "hits",
                TimeUnit.SECONDS);
        requests = APIMetrics.newMeter(getURL(url, "/requests_moving_avrage"), factory.createMetricName("Requests"),
                "requests", TimeUnit.SECONDS);
        hitRate = APIMetrics.newGauge(factory.createMetricName("HitRate"),
                new Gauge<Double>() {
                    String u = getURL(url, "/hit_rate");
                    @Override
                    public Double value() {
                        if (u == null) {
                            return 0.0;
                        }
                        return c.getDoubleValue(u);
                    }
                });
        size = APIMetrics.newGauge(factory.createMetricName("Size"),
                new Gauge<Long>() {
                    String u = getURL(url, "/size");
                    public Long value() {
                        if (u == null) {
                            return 0L;
                        }
                        return c.getLongValue(u);
                    }
                });
        entries = APIMetrics.newGauge(factory.createMetricName("Entries"),
                new Gauge<Integer>() {
                    String u = getURL(url, "/entries");
                    public Integer value() {
                        if (u == null) {
                            return 0;
                        }
                        return c.getIntValue(u);
                    }
                });
    }

    // for backward compatibility
    @Deprecated
    public double getRecentHitRate() {
        long r = requests.count();
        long h = hits.count();
        try
        {
            return ((double)(h - lastHits.get())) / (r - lastRequests.get());
        }
        finally
        {
            lastRequests.set(r);
            lastHits.set(h);
        }
    }

}
