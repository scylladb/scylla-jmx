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
 * Metrics for commit log
 */
public class CommitLogMetrics implements Metrics {
    public CommitLogMetrics() {
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        MetricNameFactory factory = new DefaultNameFactory("CommitLog");
        /** Number of completed tasks */
        registry.register(() -> registry.gauge("/commitlog/metrics/completed_tasks"),
                factory.createMetricName("CompletedTasks"));
        /** Number of pending tasks */
        registry.register(() -> registry.gauge("/commitlog/metrics/pending_tasks"),
                factory.createMetricName("PendingTasks"));
        /** Current size used by all the commit log segments */
        registry.register(() -> registry.gauge("/commitlog/metrics/total_commit_log_size"),
                factory.createMetricName("TotalCommitLogSize"));
        /**
         * Time spent waiting for a CLS to be allocated - under normal
         * conditions this should be zero
         */
        registry.register(() -> registry.timer("/commitlog/metrics/waiting_on_segment_allocation"),
                factory.createMetricName("WaitingOnSegmentAllocation"));
        /**
         * The time spent waiting on CL sync; for Periodic this is only occurs
         * when the sync is lagging its sync interval
         */
        registry.register(() -> registry.timer("/commitlog/metrics/waiting_on_commit"),
                factory.createMetricName("WaitingOnCommit"));
    }
}
