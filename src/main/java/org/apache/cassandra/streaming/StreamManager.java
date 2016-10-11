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
 * Copyright 2015 ScyllaDB
 *
 * Modified by ScyllaDB
 */

package org.apache.cassandra.streaming;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationBroadcasterSupport;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.apache.cassandra.streaming.management.StreamStateCompositeData;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMBean;

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and
 * provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming
 * status and progress.
 */
public class StreamManager extends APIMBean implements StreamManagerMBean {
    private static final Logger logger = Logger.getLogger(StreamManager.class.getName());

    private final NotificationBroadcasterSupport notifier = new NotificationBroadcasterSupport();

    public StreamManager(APIClient c) {
        super(c);
    }

    public Set<StreamState> getState() {
        JsonArray arr = client.getJsonArray("/stream_manager/");
        Set<StreamState> res = new HashSet<StreamState>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            res.add(new StreamState(obj.getString("plan_id"), obj.getString("description"),
                    SessionInfo.fromJsonArr(obj.getJsonArray("sessions"))));
        }
        return res;
    }

    @Override
    public Set<CompositeData> getCurrentStreams() {
        logger.finest("getCurrentStreams");
        return Sets
                .newHashSet(Iterables.transform(getState(), input -> StreamStateCompositeData.toCompositeData(input)));
    }

    @Override
    public void addNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) {
        notifier.addNotificationListener(listener, filter, handback);
    }

    @Override
    public void removeNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
        notifier.removeNotificationListener(listener);
    }

    @Override
    public void removeNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback)
            throws ListenerNotFoundException {
        notifier.removeNotificationListener(listener, filter, handback);
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        return notifier.getNotificationInfo();
    }
}
