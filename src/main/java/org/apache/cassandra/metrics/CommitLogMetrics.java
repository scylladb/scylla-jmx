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

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMetrics;
import com.scylladb.jmx.metrics.DefaultNameFactory;
import com.scylladb.jmx.metrics.MetricNameFactory;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Timer;

import java.util.concurrent.TimeUnit;

/**
 * Metrics for commit log
 */
public class CommitLogMetrics {
    public static final MetricNameFactory factory = new DefaultNameFactory(
            "CommitLog");
    private APIClient c = new APIClient();

    /** Number of completed tasks */
    public final Gauge<Long> completedTasks;
    /** Number of pending tasks */
    public final Gauge<Long> pendingTasks;
    /** Current size used by all the commit log segments */
    public final Gauge<Long> totalCommitLogSize;
    /**
     * Time spent waiting for a CLS to be allocated - under normal conditions
     * this should be zero
     */
    public final Timer waitingOnSegmentAllocation;
    /**
     * The time spent waiting on CL sync; for Periodic this is only occurs when
     * the sync is lagging its sync interval
     */
    public final Timer waitingOnCommit;

    public CommitLogMetrics() {
        completedTasks = APIMetrics.newGauge(
                factory.createMetricName("CompletedTasks"), new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/commitlog/metrics/completed_tasks");
                    }
                });
        pendingTasks = APIMetrics.newGauge(
                factory.createMetricName("PendingTasks"), new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/commitlog/metrics/pending_tasks");
                    }
                });
        totalCommitLogSize = APIMetrics.newGauge(
                factory.createMetricName("TotalCommitLogSize"),
                new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/commitlog/metrics/total_commit_log_size");
                    }
                });
        waitingOnSegmentAllocation = APIMetrics.newTimer("/commit_log/metrics/waiting_on_segment_allocation",
                factory.createMetricName("WaitingOnSegmentAllocation"),
                TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        waitingOnCommit = APIMetrics.newTimer("/commit_log/metrics/waiting_on_commit",
                factory.createMetricName("WaitingOnCommit"),
                TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
    }
}
