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
package org.apache.cassandra.db;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.management.*;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.cassandra.metrics.ColumnFamilyMetrics;

import com.cloudius.urchin.api.APIClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class ColumnFamilyStore implements ColumnFamilyStoreMBean {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(ColumnFamilyStore.class.getName());
    private APIClient c = new APIClient();
    private String type;
    private String keyspace;
    private String name;
    private String mbeanName;
    static final int INTERVAL = 1000; // update every 1second
    public final ColumnFamilyMetrics metric;

    private static Map<String, ColumnFamilyStore> cf = new HashMap<String, ColumnFamilyStore>();
    private static Timer timer = new Timer("Column Family");

    public void log(String str) {
        logger.info(str);
    }

    public static void register_mbeans() {
        TimerTask taskToExecute = new CheckRegistration();
        timer.scheduleAtFixedRate(taskToExecute, 100, INTERVAL);
    }

    public ColumnFamilyStore(String type, String keyspace, String name) {
        this.type = type;
        this.keyspace = keyspace;
        this.name = name;
        mbeanName = getName(type, keyspace, name);
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName nameObj = new ObjectName(mbeanName);
            mbs.registerMBean(this, nameObj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        metric = new ColumnFamilyMetrics(this);
    }

    /** true if this CFS contains secondary index data */
    /*
     * It is hard coded to false until secondary index is supported
     */
    public boolean isIndex() {
        return false;
    }

    /**
     * Get the column family name in the API format
     *
     * @return
     */
    public String getCFName() {
        return keyspace + ":" + name;
    }

    private static String getName(String type, String keyspace, String name) {
        return "org.apache.cassandra.db:type=" + type + ",keyspace=" + keyspace
                + ",columnfamily=" + name;
    }

    private static final class CheckRegistration extends TimerTask {
        private APIClient c = new APIClient();

        @Override
        public void run() {
            try {
                JsonArray mbeans = c.getJsonArray("/column_family/");
                Set<String> all_cf = new HashSet<String>();
                for (int i = 0; i < mbeans.size(); i++) {
                    JsonObject mbean = mbeans.getJsonObject(i);
                    String name = getName(mbean.getString("type"),
                            mbean.getString("ks"), mbean.getString("cf"));
                    if (!cf.containsKey(name)) {
                        ColumnFamilyStore cfs = new ColumnFamilyStore(
                                mbean.getString("type"), mbean.getString("ks"),
                                mbean.getString("cf"));
                        cf.put(name, cfs);
                    }
                    all_cf.add(name);
                }
                // removing deleted column family
                for (String n : cf.keySet()) {
                    if (!all_cf.contains(n)) {
                        cf.remove(n);
                    }
                }
            } catch (Exception e) {
                // ignoring exceptions, will retry on the next interval
            }
        }
    }

    /**
     * @return the name of the column family
     */
    public String getColumnFamilyName() {
        log(" getColumnFamilyName()");
        return name;
    }

    /**
     * Returns the total amount of data stored in the memtable, including column
     * related overhead.
     *
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#memtableOnHeapSize
     * @return The size in bytes.
     * @deprecated
     */
    @Deprecated
    public long getMemtableDataSize() {
        log(" getMemtableDataSize()");
        return c.getLongValue("");
    }

    /**
     * Returns the total number of columns present in the memtable.
     *
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#memtableColumnsCount
     * @return The number of columns.
     */
    @Deprecated
    public long getMemtableColumnsCount() {
        log(" getMemtableColumnsCount()");
        return c.getLongValue("");
    }

    /**
     * Returns the number of times that a flush has resulted in the memtable
     * being switched out.
     *
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#memtableSwitchCount
     * @return the number of memtable switches
     */
    @Deprecated
    public int getMemtableSwitchCount() {
        log(" getMemtableSwitchCount()");
        return c.getIntValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#recentSSTablesPerRead
     * @return a histogram of the number of sstable data files accessed per
     *         read: reading this property resets it
     */
    @Deprecated
    public long[] getRecentSSTablesPerReadHistogram() {
        log(" getRecentSSTablesPerReadHistogram()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#sstablesPerReadHistogram
     * @return a histogram of the number of sstable data files accessed per read
     */
    @Deprecated
    public long[] getSSTablesPerReadHistogram() {
        log(" getSSTablesPerReadHistogram()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return the number of read operations on this column family
     */
    @Deprecated
    public long getReadCount() {
        log(" getReadCount()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return total read latency (divide by getReadCount() for average)
     */
    @Deprecated
    public long getTotalReadLatencyMicros() {
        log(" getTotalReadLatencyMicros()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getLifetimeReadLatencyHistogramMicros() {
        log(" getLifetimeReadLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getRecentReadLatencyHistogramMicros() {
        log(" getRecentReadLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#readLatency
     * @return average latency per read operation since the last call
     */
    @Deprecated
    public double getRecentReadLatencyMicros() {
        log(" getRecentReadLatencyMicros()");
        return c.getDoubleValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return the number of write operations on this column family
     */
    @Deprecated
    public long getWriteCount() {
        log(" getWriteCount()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return total write latency (divide by getReadCount() for average)
     */
    @Deprecated
    public long getTotalWriteLatencyMicros() {
        log(" getTotalWriteLatencyMicros()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getLifetimeWriteLatencyHistogramMicros() {
        log(" getLifetimeWriteLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return an array representing the latency histogram
     */
    @Deprecated
    public long[] getRecentWriteLatencyHistogramMicros() {
        log(" getRecentWriteLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#writeLatency
     * @return average latency per write operation since the last call
     */
    @Deprecated
    public double getRecentWriteLatencyMicros() {
        log(" getRecentWriteLatencyMicros()");
        return c.getDoubleValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#pendingFlushes
     * @return the estimated number of tasks pending for this column family
     */
    @Deprecated
    public int getPendingTasks() {
        log(" getPendingTasks()");
        return c.getIntValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#liveSSTableCount
     * @return the number of SSTables on disk for this CF
     */
    @Deprecated
    public int getLiveSSTableCount() {
        log(" getLiveSSTableCount()");
        return c.getIntValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#liveDiskSpaceUsed
     * @return disk space used by SSTables belonging to this CF
     */
    @Deprecated
    public long getLiveDiskSpaceUsed() {
        log(" getLiveDiskSpaceUsed()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#totalDiskSpaceUsed
     * @return total disk space used by SSTables belonging to this CF, including
     *         obsolete ones waiting to be GC'd
     */
    @Deprecated
    public long getTotalDiskSpaceUsed() {
        log(" getTotalDiskSpaceUsed()");
        return c.getLongValue("");
    }

    /**
     * force a major compaction of this column family
     */
    public void forceMajorCompaction()
            throws ExecutionException, InterruptedException {
        log(" forceMajorCompaction() throws ExecutionException, InterruptedException");
        c.post("column_family/major_compaction/" + getCFName());
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#minRowSize
     * @return the size of the smallest compacted row
     */
    @Deprecated
    public long getMinRowSize() {
        log(" getMinRowSize()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#maxRowSize
     * @return the size of the largest compacted row
     */
    @Deprecated
    public long getMaxRowSize() {
        log(" getMaxRowSize()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#meanRowSize
     * @return the average row size across all the sstables
     */
    @Deprecated
    public long getMeanRowSize() {
        log(" getMeanRowSize()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterFalsePositives
     */
    @Deprecated
    public long getBloomFilterFalsePositives() {
        log(" getBloomFilterFalsePositives()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#recentBloomFilterFalsePositives
     */
    @Deprecated
    public long getRecentBloomFilterFalsePositives() {
        log(" getRecentBloomFilterFalsePositives()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterFalseRatio
     */
    @Deprecated
    public double getBloomFilterFalseRatio() {
        log(" getBloomFilterFalseRatio()");
        return c.getDoubleValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#recentBloomFilterFalseRatio
     */
    @Deprecated
    public double getRecentBloomFilterFalseRatio() {
        log(" getRecentBloomFilterFalseRatio()");
        return c.getDoubleValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterDiskSpaceUsed
     */
    @Deprecated
    public long getBloomFilterDiskSpaceUsed() {
        log(" getBloomFilterDiskSpaceUsed()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#bloomFilterOffHeapMemoryUsed
     */
    @Deprecated
    public long getBloomFilterOffHeapMemoryUsed() {
        log(" getBloomFilterOffHeapMemoryUsed()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#indexSummaryOffHeapMemoryUsed
     */
    @Deprecated
    public long getIndexSummaryOffHeapMemoryUsed() {
        log(" getIndexSummaryOffHeapMemoryUsed()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#compressionMetadataOffHeapMemoryUsed
     */
    @Deprecated
    public long getCompressionMetadataOffHeapMemoryUsed() {
        log(" getCompressionMetadataOffHeapMemoryUsed()");
        return c.getLongValue("");
    }

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    public int getMinimumCompactionThreshold() {
        log(" getMinimumCompactionThreshold()");
        return c.getIntValue("column_family/minimum_compaction/" + getCFName());
    }

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    public void setMinimumCompactionThreshold(int threshold) {
        log(" setMinimumCompactionThreshold(int threshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("value", Integer.toString(threshold));
        c.post("column_family/minimum_compaction/" + getCFName(), queryParams);
    }

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    public int getMaximumCompactionThreshold() {
        log(" getMaximumCompactionThreshold()");
        return c.getIntValue("column_family/maximum_compaction/" + getCFName());
    }

    /**
     * Sets the maximum and maximum number of SSTables in queue before
     * compaction kicks off
     */
    public void setCompactionThresholds(int minThreshold, int maxThreshold) {
        log(" setCompactionThresholds(int minThreshold, int maxThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("minimum", Integer.toString(minThreshold));
        queryParams.add("maximum", Integer.toString(maxThreshold));
        c.post("column_family/compaction" + getCFName(), queryParams);
    }

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    public void setMaximumCompactionThreshold(int threshold) {
        log(" setMaximumCompactionThreshold(int threshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("value", Integer.toString(threshold));
        c.post("column_family/maximum_compaction/" + getCFName(), queryParams);
    }

    /**
     * Sets the compaction strategy by class name
     *
     * @param className
     *            the name of the compaction strategy class
     */
    public void setCompactionStrategyClass(String className) {
        log(" setCompactionStrategyClass(String className)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("class_name", className);
        c.post("column_family/compaction_strategy/" + getCFName(), queryParams);
    }

    /**
     * Gets the compaction strategy class name
     */
    public String getCompactionStrategyClass() {
        log(" getCompactionStrategyClass()");
        return c.getStringValue(
                "column_family/compaction_strategy/" + getCFName());
    }

    /**
     * Get the compression parameters
     */
    public Map<String, String> getCompressionParameters() {
        log(" getCompressionParameters()");
        return c.getMapStrValue(
                "column_family/compression_parameters/" + getCFName());
    }

    /**
     * Set the compression parameters
     *
     * @param opts
     *            map of string names to values
     */
    public void setCompressionParameters(Map<String, String> opts) {
        log(" setCompressionParameters(Map<String,String> opts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("opts", APIClient.mapToString(opts));
        c.post("column_family/compression_parameters/" + getCFName(),
                queryParams);
    }

    /**
     * Set new crc check chance
     */
    public void setCrcCheckChance(double crcCheckChance) {
        log(" setCrcCheckChance(double crcCheckChance)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("check_chance", Double.toString(crcCheckChance));
        c.post("column_family/crc_check_chance/" + getCFName(), queryParams);
    }

    public boolean isAutoCompactionDisabled() {
        log(" isAutoCompactionDisabled()");
        return c.getBooleanValue("column_family/autocompaction/" + getCFName());
    }

    /** Number of tombstoned cells retreived during the last slicequery */
    @Deprecated
    public double getTombstonesPerSlice() {
        log(" getTombstonesPerSlice()");
        return c.getDoubleValue("");
    }

    /** Number of live cells retreived during the last slicequery */
    @Deprecated
    public double getLiveCellsPerSlice() {
        log(" getLiveCellsPerSlice()");
        return c.getDoubleValue("");
    }

    public long estimateKeys() {
        log(" estimateKeys()");
        return c.getLongValue("column_family/estimate_keys/" + getCFName());
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#estimatedRowSizeHistogram
     */
    @Deprecated
    public long[] getEstimatedRowSizeHistogram() {
        log(" getEstimatedRowSizeHistogram()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#estimatedColumnCountHistogram
     */
    @Deprecated
    public long[] getEstimatedColumnCountHistogram() {
        log(" getEstimatedColumnCountHistogram()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.ColumnFamilyMetrics#compressionRatio
     */
    @Deprecated
    public double getCompressionRatio() {
        log(" getCompressionRatio()");
        return c.getDoubleValue("");
    }

    /**
     * Returns a list of the names of the built column indexes for current store
     *
     * @return list of the index names
     */
    public List<String> getBuiltIndexes() {
        log(" getBuiltIndexes()");
        return c.getListStrValue("column_family/built_indexes/" + getCFName());
    }

    /**
     * Returns a list of filenames that contain the given key on this node
     *
     * @param key
     * @return list of filenames containing the key
     */
    public List<String> getSSTablesForKey(String key) {
        log(" getSSTablesForKey(String key)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("key", key);
        return c.getListStrValue("column_family/sstables/by_key/" + getCFName(),
                queryParams);
    }

    /**
     * Scan through Keyspace/ColumnFamily's data directory determine which
     * SSTables should be loaded and load them
     */
    public void loadNewSSTables() {
        log(" loadNewSSTables()");
        c.post("column_family/sstable/" + getCFName());
    }

    /**
     * @return the number of SSTables in L0. Always return 0 if Leveled
     *         compaction is not enabled.
     */
    public int getUnleveledSSTables() {
        log(" getUnleveledSSTables()");
        return c.getIntValue("column_family/sstables/unleveled/" + getCFName());
    }

    /**
     * @return sstable count for each level. null unless leveled compaction is
     *         used. array index corresponds to level(int[0] is for level 0,
     *         ...).
     */
    public int[] getSSTableCountPerLevel() {
        log(" getSSTableCountPerLevel()");
        return c.getIntArrValue(
                "column_family/sstables/per_level/" + getCFName());
    }

    /**
     * Get the ratio of droppable tombstones to real columns (and non-droppable
     * tombstones)
     *
     * @return ratio
     */
    public double getDroppableTombstoneRatio() {
        log(" getDroppableTombstoneRatio()");
        return c.getDoubleValue("column_family/droppable_ratio/" + getCFName());
    }

    /**
     * @return the size of SSTables in "snapshots" subdirectory which aren't
     *         live anymore
     */
    public long trueSnapshotsSize() {
        log(" trueSnapshotsSize()");
        return c.getLongValue("column_family/snapshots_size/" + getCFName());
    }

    public String getKeyspace() {
        return keyspace;
    }

}
