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
package org.apache.cassandra.gms;

import java.util.HashMap;
import java.util.Map;

/**
 * This abstraction represents both the HeartBeatState and the ApplicationState
 * in an EndpointState instance. Any state for a given endpoint can be retrieved
 * from this instance.
 */

public class EndpointState {
    private volatile HeartBeatState hbState;

    final Map<ApplicationState, String> applicationState = new HashMap<ApplicationState, String>();

    private volatile long updateTimestamp;
    private volatile boolean isAlive;
    ApplicationState[] applicationValues;
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(EndpointState.class.getName());

    EndpointState(HeartBeatState initialHbState) {
        applicationValues = ApplicationState.values();
        hbState = initialHbState;
        updateTimestamp = System.nanoTime();
        isAlive = true;
    }

    HeartBeatState getHeartBeatState() {
        return hbState;
    }

    void setHeartBeatState(HeartBeatState newHbState) {
        hbState = newHbState;
    }

    public String getApplicationState(ApplicationState key) {
        return applicationState.get(key);
    }

    /**
     * TODO replace this with operations that don't expose private state
     */
    @Deprecated
    public Map<ApplicationState, String> getApplicationStateMap() {
        return applicationState;
    }

    void addApplicationState(ApplicationState key, String value) {
        applicationState.put(key, value);
    }

    void addApplicationState(int key, String value) {
        if (key >= applicationValues.length) {
            logger.warning("Unknown application state with id:" + key);
            return;
        }
        addApplicationState(applicationValues[key], value);
    }

    /* getters and setters */
    /**
     * @return System.nanoTime() when state was updated last time.
     */
    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long ts) {
        updateTimestamp = ts;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public void setAliave(boolean alive) {
        isAlive = alive;
    }

    @Override
    public String toString() {
        return "EndpointState: HeartBeatState = " + hbState + ", AppStateMap = " + applicationState;
    }
}
