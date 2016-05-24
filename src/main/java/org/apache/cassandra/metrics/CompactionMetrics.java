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

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMetrics;
import com.scylladb.jmx.metrics.DefaultNameFactory;
import com.scylladb.jmx.metrics.MetricNameFactory;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.APIMeter;

/**
 * Metrics for compaction.
 */
public class CompactionMetrics {
    public static final MetricNameFactory factory = new DefaultNameFactory(
            "Compaction");
    private APIClient c = new APIClient();
    /** Estimated number of compactions remaining to perform */
    public final Gauge<Integer> pendingTasks;
    /** Number of completed compactions since server [re]start */
    public final Gauge<Long> completedTasks;
    /** Total number of compactions since server [re]start */
    public final APIMeter totalCompactionsCompleted;
    /** Total number of bytes compacted since server [re]start */
    public final Counter bytesCompacted;

    public CompactionMetrics() {

        pendingTasks = APIMetrics.newGauge(
                factory.createMetricName("PendingTasks"), new Gauge<Integer>() {
                    public Integer value() {
                        return c.getIntValue("/compaction_manager/metrics/pending_tasks");
                    }
                });
        completedTasks = APIMetrics.newGauge(
                factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/compaction_manager/metrics/completed_tasks");
                    }
                });
        totalCompactionsCompleted = APIMetrics.newMeter(
                "/compaction_manager/metrics/total_compactions_completed",
                factory.createMetricName("TotalCompactionsCompleted"),
                "compaction completed", TimeUnit.SECONDS);
        bytesCompacted = APIMetrics.newCounter(
                "/compaction_manager/metrics/bytes_compacted",
                factory.createMetricName("BytesCompacted"));
    }
}
