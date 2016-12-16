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
 * Metrics related to Storage.
 */
public class StorageMetrics implements Metrics {
    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        MetricNameFactory factory = new DefaultNameFactory("Storage");
        registry.register(() -> registry.counter("/storage_service/metrics/load"), factory.createMetricName("Load"));
        registry.register(() -> registry.counter("/storage_service/metrics/exceptions"),
                factory.createMetricName("Exceptions"));
        registry.register(() -> registry.counter("/storage_service/metrics/hints_in_progress"),
                factory.createMetricName("TotalHintsInProgress"));
        registry.register(() -> registry.counter("/storage_service/metrics/total_hints"),
                factory.createMetricName("TotalHints"));
    }
}
