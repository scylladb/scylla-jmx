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
package org.apache.cassandra.db.commitlog;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.metrics.CommitLogMetrics;

import com.cloudius.urchin.api.APIClient;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog implements CommitLogMBean {

    CommitLogMetrics metrics = new CommitLogMetrics();
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(CommitLog.class.getName());

    private APIClient c = new APIClient();

    public void log(String str) {
        logger.info(str);
    }

    private static final CommitLog instance = new CommitLog();

    public static CommitLog getInstance() {
        return instance;
    }

    private CommitLog() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName(
                    "org.apache.cassandra.db:type=Commitlog"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the number of completed tasks
     *
     * @see org.apache.cassandra.metrics.CommitLogMetrics#completedTasks
     */
    @Deprecated
    public long getCompletedTasks() {
        log(" getCompletedTasks()");
        return c.getLongValue("");
    }

    /**
     * Get the number of tasks waiting to be executed
     *
     * @see org.apache.cassandra.metrics.CommitLogMetrics#pendingTasks
     */
    @Deprecated
    public long getPendingTasks() {
        log(" getPendingTasks()");
        return c.getLongValue("");
    }

    /**
     * Get the current size used by all the commitlog segments.
     *
     * @see org.apache.cassandra.metrics.CommitLogMetrics#totalCommitLogSize
     */
    @Deprecated
    public long getTotalCommitlogSize() {
        log(" getTotalCommitlogSize()");
        return c.getLongValue("");
    }

    /**
     * Recover a single file.
     */
    public void recover(String path) throws IOException {
        log(" recover(String path) throws IOException");
    }

    /**
     * @return file names (not full paths) of active commit log segments
     *         (segments containing unflushed data)
     */
    public List<String> getActiveSegmentNames() {
        log(" getActiveSegmentNames()");
        List<String> lst = c.getListStrValue("/commitlog/segments/active");
        Set<String> set = new HashSet<String>();
        for (String l : lst) {
            String name = l.substring(l.lastIndexOf("/") + 1, l.length());
            set.add(name);
        }
        return new ArrayList<String>(set);
    }

    /**
     * @return Files which are pending for archival attempt. Does NOT include
     *         failed archive attempts.
     */
    public List<String> getArchivingSegmentNames() {
        log(" getArchivingSegmentNames()");
        List<String> lst = c.getListStrValue("/commitlog/segments/archiving");
        Set<String> set = new HashSet<String>();
        for (String l : lst) {
            String name = l.substring(l.lastIndexOf("/") + 1, l.length());
            set.add(name);
        }
        return new ArrayList<String>(set);
    }

}
