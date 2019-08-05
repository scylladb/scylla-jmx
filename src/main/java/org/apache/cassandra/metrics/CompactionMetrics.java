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

import java.util.HashMap;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.MalformedObjectNameException;

/**
 * Metrics for compaction.
 */
public class CompactionMetrics implements Metrics {
    public CompactionMetrics() {
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        MetricNameFactory factory = new DefaultNameFactory("Compaction");
        /** Estimated number of compactions remaining to perform */
        registry.register(() -> registry.gauge(Integer.class, "/compaction_manager/metrics/pending_tasks"),
                factory.createMetricName("PendingTasks"));
        /** Number of completed compactions since server [re]start */
        registry.register(() -> registry.gauge("/compaction_manager/metrics/completed_tasks"),
                factory.createMetricName("CompletedTasks"));
        /** Total number of compactions since server [re]start */
        registry.register(() -> registry.meter("/compaction_manager/metrics/total_compactions_completed"),
                factory.createMetricName("TotalCompactionsCompleted"));
        /** Total number of bytes compacted since server [re]start */
        registry.register(() -> registry.meter("/compaction_manager/metrics/bytes_compacted"),
                factory.createMetricName("BytesCompacted"));

        registry.register(() -> registry.gauge((client) -> {
            Map<String, Map<String, Integer>> result = new HashMap<>();
            JsonArray compactions = client.getJsonArray("compaction_manager/metrics/pending_tasks_by_table");

            for (int i = 0; i < compactions.size(); i++) {
                JsonObject c = compactions.getJsonObject(i);

                String ks = c.getString("ks");
                String cf = c.getString("cf");

                if (!result.containsKey(ks)) {
                    result.put(ks, new HashMap<>());
                }

                Map<String, Integer> map = result.get(ks);
                map.put(cf, (int)(c.getJsonNumber("task").longValue()));
            }
            return result;
        }), factory.createMetricName("PendingTasksByTableName"));
    }
}
