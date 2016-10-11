/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

package org.apache.cassandra.metrics;

import javax.management.MalformedObjectNameException;

// TODO: In StorageProxy
public class CASClientRequestMetrics extends ClientRequestMetrics {

    public CASClientRequestMetrics(String scope, String url) {
        super(scope, url);
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        super.register(registry);
        registry.register(() -> registry.histogram(uri + "/contention", true), names("ContentionHistogram"));
        registry.register(() -> registry.counter(uri + "/condition_not_met"), names("ConditionNotMet"));
        registry.register(() -> registry.counter(uri + "/unfinished_commit"), names("UnfinishedCommit"));
    }
}
