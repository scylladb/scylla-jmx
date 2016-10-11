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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.metrics.CommitLogMetrics;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.MetricsMBean;

/*
 * Commit Log tracks every write operation into the system. The aim of the commit log is to be able to
 * successfully recover data that was not stored to disk via the Memtable.
 */
public class CommitLog extends MetricsMBean implements CommitLogMBean {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(CommitLog.class.getName());

    public void log(String str) {
        logger.finest(str);
    }

    public CommitLog(APIClient client) {
        super("org.apache.cassandra.db:type=Commitlog", client, new CommitLogMetrics());
    }

    /**
     * Recover a single file.
     */
    @Override
    public void recover(String path) throws IOException {
        log(" recover(String path) throws IOException");
    }

    /**
     * @return file names (not full paths) of active commit log segments
     *         (segments containing unflushed data)
     */
    @Override
    public List<String> getActiveSegmentNames() {
        log(" getActiveSegmentNames()");
        List<String> lst = client.getListStrValue("/commitlog/segments/active");
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
    @Override
    public List<String> getArchivingSegmentNames() {
        log(" getArchivingSegmentNames()");
        List<String> lst = client.getListStrValue("/commitlog/segments/archiving");
        Set<String> set = new HashSet<String>();
        for (String l : lst) {
            String name = l.substring(l.lastIndexOf("/") + 1, l.length());
            set.add(name);
        }
        return new ArrayList<String>(set);
    }

    @Override
    public String getArchiveCommand() {
        // TODO Auto-generated method stub
        log(" getArchiveCommand()");
        return client.getStringValue("");
    }

    @Override
    public String getRestoreCommand() {
        // TODO Auto-generated method stub
        log(" getRestoreCommand()");
        return client.getStringValue("");
    }

    @Override
    public String getRestoreDirectories() {
        // TODO Auto-generated method stub
        log(" getRestoreDirectories()");
        return client.getStringValue("");
    }

    @Override
    public long getRestorePointInTime() {
        // TODO Auto-generated method stub
        log(" getRestorePointInTime()");
        return client.getLongValue("");
    }

    @Override
    public String getRestorePrecision() {
        // TODO Auto-generated method stub
        log(" getRestorePrecision()");
        return client.getStringValue("");
    }

    @Override
    public long getActiveContentSize() {
        // scylla does not compress commit log, so this is equivalent
        return getActiveOnDiskSize();
    }

    @Override
    public long getActiveOnDiskSize() {
        return client.getLongValue("/commitlog/metrics/total_commit_log_size");
    }

    @Override
    public Map<String, Double> getActiveSegmentCompressionRatios() {
        HashMap<String, Double> res = new HashMap<>();
        for (String name : getActiveSegmentNames()) {
            res.put(name, 1.0);
        }
        return res;
    }
}
