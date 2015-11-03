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

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanNotificationInfo;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import org.apache.cassandra.streaming.management.StreamStateCompositeData;

import com.cloudius.urchin.api.APIClient;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * StreamManager manages currently running {@link StreamResultFuture}s and
 * provides status of all operation invoked.
 *
 * All stream operation should be created through this class to track streaming
 * status and progress.
 */
public class StreamManager implements StreamManagerMBean {
    public static final StreamManager instance = new StreamManager();
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(StreamManager.class.getName());
    private APIClient c = new APIClient();

    public Set<StreamState> getState() {
        JsonArray arr = c.getJsonArray("/stream_manager/");
        Set<StreamState> res = new HashSet<StreamState>();
        for (int i = 0; i < arr.size(); i++) {
            JsonObject obj = arr.getJsonObject(i);
            res.add(new StreamState(obj.getString("plan_id"), obj.getString("description"), SessionInfo.fromJsonArr(obj.getJsonArray("sessions"))));
        }
        return res;
    }

    public static StreamManager getInstance() {
        return instance;
    }
    public Set<CompositeData> getCurrentStreams() {
        logger.info("getCurrentStreams");
        return Sets.newHashSet(Iterables.transform(getState(), new Function<StreamState, CompositeData>()
        {
            public CompositeData apply(StreamState input)
            {
                return StreamStateCompositeData.toCompositeData(input);
            }
        }));
    }

    @Override
    public void removeNotificationListener(NotificationListener arg0,
            NotificationFilter arg1, Object arg2)
                    throws ListenerNotFoundException {
        // TODO Auto-generated method stub

    }

    @Override
    public void addNotificationListener(NotificationListener arg0,
            NotificationFilter arg1, Object arg2)
                    throws IllegalArgumentException {
        // TODO Auto-generated method stub

    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void removeNotificationListener(NotificationListener arg0)
            throws ListenerNotFoundException {
        // TODO Auto-generated method stub

    }
}
