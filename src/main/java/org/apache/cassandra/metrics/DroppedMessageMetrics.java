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
 * Copyright (C) 2015 ScyllaDB
 */
/*
 * Moddified by ScyllaDB
 */

package org.apache.cassandra.metrics;

import javax.management.MalformedObjectNameException;

import org.apache.cassandra.net.MessagingService;

/**
 * Metrics for dropped messages by verb.
 */
public class DroppedMessageMetrics implements Metrics {
    private final MessagingService.Verb verb;

    public DroppedMessageMetrics(MessagingService.Verb verb) {
        this.verb = verb;
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        MetricNameFactory factory = new DefaultNameFactory("DroppedMessage", verb.toString());
        /** Number of dropped messages */
        // TODO: this API url does not exist. Add meter calls for verbs.
        registry.register(() -> registry.meter("/messaging_service/messages/dropped/" + verb),
                factory.createMetricName("Dropped"));

    }
}
