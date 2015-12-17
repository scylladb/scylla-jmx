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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamilyStore;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.APIMetrics;
import com.scylladb.jmx.metrics.MetricNameFactory;
import com.scylladb.jmx.utils.RecentEstimatedHistogram;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.*;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class ColumnFamilyMetrics {
    private APIClient c = new APIClient();
    /**
     * Total amount of data stored in the memtable that resides on-heap,
     * including column related overhead and overwritten rows.
     */
    public final Gauge<Long> memtableOnHeapSize;
    /**
     * Total amount of data stored in the memtable that resides off-heap,
     * including column related overhead and overwritten rows.
     */
    public final Gauge<Long> memtableOffHeapSize;
    /**
     * Total amount of live data stored in the memtable, excluding any data
     * structure overhead
     */
    public final Gauge<Long> memtableLiveDataSize;
    /**
     * Total amount of data stored in the memtables (2i and pending flush
     * memtables included) that resides on-heap.
     */
    public final Gauge<Long> allMemtablesOnHeapSize;
    /**
     * Total amount of data stored in the memtables (2i and pending flush
     * memtables included) that resides off-heap.
     */
    public final Gauge<Long> allMemtablesOffHeapSize;
    /**
     * Total amount of live data stored in the memtables (2i and pending flush
     * memtables included) that resides off-heap, excluding any data structure
     * overhead
     */
    public final Gauge<Long> allMemtablesLiveDataSize;
    /** Total number of columns present in the memtable. */
    public final Gauge<Long> memtableColumnsCount;
    /** Number of times flush has resulted in the memtable being switched out. */
    public final Counter memtableSwitchCount;
    /** Current compression ratio for all SSTables */
    public final Gauge<Double> compressionRatio;
    /** Histogram of estimated row size (in bytes). */
    public final Gauge<long[]> estimatedRowSizeHistogram;
    /** Approximate number of keys in table. */
    public final Gauge<Long> estimatedRowCount;
    /** Histogram of estimated number of columns. */
    public final Gauge<long[]> estimatedColumnCountHistogram;
    /** Histogram of the number of sstable data files accessed per read */
    public final ColumnFamilyHistogram sstablesPerReadHistogram;
    /** (Local) read metrics */
    public final LatencyMetrics readLatency;
    /** (Local) range slice metrics */
    public final LatencyMetrics rangeLatency;
    /** (Local) write metrics */
    public final LatencyMetrics writeLatency;
    /** Estimated number of tasks pending for this column family */
    public final Counter pendingFlushes;
    /** Estimate of number of pending compactios for this CF */
    public final Gauge<Integer> pendingCompactions;
    /** Number of SSTables on disk for this CF */
    public final Gauge<Integer> liveSSTableCount;
    /** Disk space used by SSTables belonging to this CF */
    public final Counter liveDiskSpaceUsed;
    /**
     * Total disk space used by SSTables belonging to this CF, including
     * obsolete ones waiting to be GC'd
     */
    public final Counter totalDiskSpaceUsed;
    /** Size of the smallest compacted row */
    public final Gauge<Long> minRowSize;
    /** Size of the largest compacted row */
    public final Gauge<Long> maxRowSize;
    /** Size of the smallest compacted row */
    public final Gauge<Long> meanRowSize;
    /** Number of false positives in bloom filter */
    public final Gauge<Long> bloomFilterFalsePositives;
    /** Number of false positives in bloom filter from last read */
    public final Gauge<Long> recentBloomFilterFalsePositives;
    /** False positive ratio of bloom filter */
    public final Gauge<Double> bloomFilterFalseRatio;
    /** False positive ratio of bloom filter from last read */
    public final Gauge<Double> recentBloomFilterFalseRatio;
    /** Disk space used by bloom filter */
    public final Gauge<Long> bloomFilterDiskSpaceUsed;
    /** Off heap memory used by bloom filter */
    public final Gauge<Long> bloomFilterOffHeapMemoryUsed;
    /** Off heap memory used by index summary */
    public final Gauge<Long> indexSummaryOffHeapMemoryUsed;
    /** Off heap memory used by compression meta data */
    public final Gauge<Long> compressionMetadataOffHeapMemoryUsed;
    /** Key cache hit rate for this CF */
    public final Gauge<Double> keyCacheHitRate;
    /** Tombstones scanned in queries on this CF */
    public final ColumnFamilyHistogram tombstoneScannedHistogram;
    /** Live cells scanned in queries on this CF */
    public final ColumnFamilyHistogram liveScannedHistogram;
    /** Column update time delta on this CF */
    public final ColumnFamilyHistogram colUpdateTimeDeltaHistogram;
    /** Disk space used by snapshot files which */
    public final Gauge<Long> trueSnapshotsSize;
    /** Row cache hits, but result out of range */
    public final Counter rowCacheHitOutOfRange;
    /** Number of row cache hits */
    public final Counter rowCacheHit;
    /** Number of row cache misses */
    public final Counter rowCacheMiss;
    /** CAS Prepare metrics */
    public final LatencyMetrics casPrepare;
    /** CAS Propose metrics */
    public final LatencyMetrics casPropose;
    /** CAS Commit metrics */
    public final LatencyMetrics casCommit;

    public final Timer coordinatorReadLatency;
    public final Timer coordinatorScanLatency;

    /** Time spent waiting for free memtable space, either on- or off-heap */
    public final Timer waitingOnFreeMemtableSpace;

    private final MetricNameFactory factory;
    private static final MetricNameFactory globalNameFactory = new AllColumnFamilyMetricNameFactory();

    public final Counter speculativeRetries;

    // for backward compatibility
    @Deprecated
    public final EstimatedHistogramWrapper sstablesPerRead;
    // it should not be called directly
    @Deprecated
    protected final RecentEstimatedHistogram recentSSTablesPerRead = new RecentEstimatedHistogram(35);
    private String cfName;

    public final static LatencyMetrics globalReadLatency = new LatencyMetrics(
            "/column_family/metrics/read_latency", globalNameFactory, "Read");
    public final static LatencyMetrics globalWriteLatency = new LatencyMetrics(
            "/column_family/metrics/write_latency", globalNameFactory, "Write");
    public final static LatencyMetrics globalRangeLatency = new LatencyMetrics(
            "/column_family/metrics/range_latency", globalNameFactory, "Range");

    /**
     * stores metrics that will be rolled into a single global metric
     */
    public final static ConcurrentMap<String, Set<Metric>> allColumnFamilyMetrics = Maps
            .newConcurrentMap();

    /**
     * Stores all metric names created that can be used when unregistering
     */
    public final static Set<String> all = Sets.newHashSet();

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs
     *            ColumnFamilyStore to measure metrics
     */
    public ColumnFamilyMetrics(final ColumnFamilyStore cfs) {
        factory = new ColumnFamilyMetricNameFactory(cfs);
        cfName = cfs.getCFName();
        memtableColumnsCount = createColumnFamilyGauge(
                "/column_family/metrics/memtable_columns_count",
                "MemtableColumnsCount");
        memtableOnHeapSize = createColumnFamilyGauge(
                "/column_family/metrics/memtable_on_heap_size",
                "MemtableOnHeapSize");
        memtableOffHeapSize = createColumnFamilyGauge(
                "/column_family/metrics/memtable_off_heap_size",
                "MemtableOffHeapSize");
        memtableLiveDataSize = createColumnFamilyGauge(
                "/column_family/metrics/memtable_live_data_size",
                "MemtableLiveDataSize");
        allMemtablesOnHeapSize = createColumnFamilyGauge(
                "/column_family/metrics/all_memtables_on_heap_size",
                "AllMemtablesHeapSize");
        allMemtablesOffHeapSize = createColumnFamilyGauge(
                "/column_family/metrics/all_memtables_off_heap_size",
                "AllMemtablesOffHeapSize");
        allMemtablesLiveDataSize = createColumnFamilyGauge(
                "/column_family/metrics/all_memtables_live_data_size",
                "AllMemtablesLiveDataSize");
        memtableSwitchCount = createColumnFamilyCounter(
                "/column_family/metrics/memtable_switch_count",
                "MemtableSwitchCount");
        estimatedRowSizeHistogram = Metrics.newGauge(
                factory.createMetricName("EstimatedRowSizeHistogram"),
                new Gauge<long[]>() {
                    public long[] value() {
                        return c.getEstimatedHistogramAsLongArrValue("/column_family/metrics/estimated_row_size_histogram/"
                                + cfName);
                    }
                });
        estimatedRowCount= Metrics.newGauge(
                factory.createMetricName("EstimatedRowCount"),
                new Gauge<Long>() {
                    public Long value() {
                        return c.getLongValue("/column_family/metrics/estimated_row_count/"
                                + cfName);
                    }
                });

        estimatedColumnCountHistogram = Metrics.newGauge(
                factory.createMetricName("EstimatedColumnCountHistogram"),
                new Gauge<long[]>() {
                    public long[] value() {
                        return c.getEstimatedHistogramAsLongArrValue("/column_family/metrics/estimated_column_count_histogram/"
                                + cfName);
                    }
                });
        sstablesPerReadHistogram = createColumnFamilyHistogram(
                "/column_family/metrics/sstables_per_read_histogram",
                "SSTablesPerReadHistogram");
        compressionRatio = createColumnFamilyGauge("CompressionRatio",
                new Gauge<Double>() {
                    public Double value() {
                        return c.getDoubleValue("/column_family/metrics/compression_ratio/"
                                + cfName);
                    }
                }, new Gauge<Double>() // global gauge
                {
                    public Double value() {
                        return c.getDoubleValue("/column_family/metrics/compression_ratio/");
                    }
                });
        readLatency = new LatencyMetrics("/column_family/metrics/read_latency",
                cfName, factory, "Read");
        writeLatency = new LatencyMetrics(
                "/column_family/metrics/write_latency", cfName, factory,
                "Write");
        rangeLatency = new LatencyMetrics(
                "/column_family/metrics/range_latency", cfName, factory,
                "Range");
        pendingFlushes = createColumnFamilyCounter(
                "/column_family/metrics/pending_flushes", "PendingFlushes");
        pendingCompactions = createColumnFamilyGaugeInt(
                "/column_family/metrics/pending_compactions",
                "PendingCompactions");
        liveSSTableCount = createColumnFamilyGaugeInt(
                "/column_family/metrics/live_ss_table_count",
                "LiveSSTableCount");
        liveDiskSpaceUsed = createColumnFamilyCounter(
                "/column_family/metrics/live_disk_space_used",
                "LiveDiskSpaceUsed");
        totalDiskSpaceUsed = createColumnFamilyCounter(
                "/column_family/metrics/total_disk_space_used",
                "TotalDiskSpaceUsed");
        minRowSize = createColumnFamilyGauge(
                "/column_family/metrics/min_row_size", "MinRowSize");
        maxRowSize = createColumnFamilyGauge(
                "/column_family/metrics/max_row_size", "MaxRowSize");
        meanRowSize = createColumnFamilyGauge(
                "/column_family/metrics/mean_row_size", "MeanRowSize");
        bloomFilterFalsePositives = createColumnFamilyGauge(
                "/column_family/metrics/bloom_filter_false_positives",
                "BloomFilterFalsePositives");
        recentBloomFilterFalsePositives = createColumnFamilyGauge(
                "/column_family/metrics/recent_bloom_filter_false_positives",
                "RecentBloomFilterFalsePositives");
        bloomFilterFalseRatio = createColumnFamilyGaugeDouble(
                "/column_family/metrics/bloom_filter_false_ratio",
                "BloomFilterFalseRatio");
        recentBloomFilterFalseRatio = createColumnFamilyGaugeDouble(
                "/column_family/metrics/recent_bloom_filter_false_ratio",
                "RecentBloomFilterFalseRatio");
        bloomFilterDiskSpaceUsed = createColumnFamilyGauge(
                "/column_family/metrics/bloom_filter_disk_space_used",
                "BloomFilterDiskSpaceUsed");
        bloomFilterOffHeapMemoryUsed = createColumnFamilyGauge(
                "/column_family/metrics/bloom_filter_off_heap_memory_used",
                "BloomFilterOffHeapMemoryUsed");
        indexSummaryOffHeapMemoryUsed = createColumnFamilyGauge(
                "/column_family/metrics/index_summary_off_heap_memory_used",
                "IndexSummaryOffHeapMemoryUsed");
        compressionMetadataOffHeapMemoryUsed = createColumnFamilyGauge(
                "/column_family/metrics/compression_metadata_off_heap_memory_used",
                "CompressionMetadataOffHeapMemoryUsed");
        speculativeRetries = createColumnFamilyCounter(
                "/column_family/metrics/speculative_retries",
                "SpeculativeRetries");
        keyCacheHitRate = Metrics.newGauge(
                factory.createMetricName("KeyCacheHitRate"),
                new Gauge<Double>() {
                    @Override
                    public Double value() {
                        return c.getDoubleValue("/column_family/metrics/key_cache_hit_rate/"
                                + cfName);
                    }
                });
        tombstoneScannedHistogram = createColumnFamilyHistogram(
                "/column_family/metrics/tombstone_scanned_histogram",
                "TombstoneScannedHistogram");
        liveScannedHistogram = createColumnFamilyHistogram(
                "/column_family/metrics/live_scanned_histogram",
                "LiveScannedHistogram");
        colUpdateTimeDeltaHistogram = createColumnFamilyHistogram(
                "/column_family/metrics/col_update_time_delta_histogram",
                "ColUpdateTimeDeltaHistogram");
        coordinatorReadLatency = APIMetrics.newTimer("/column_family/metrics/coordinator/read/" + cfName,
                factory.createMetricName("CoordinatorReadLatency"),
                TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        coordinatorScanLatency = APIMetrics.newTimer("/column_family/metrics/coordinator/scan/" + cfName,
                factory.createMetricName("CoordinatorScanLatency"),
                TimeUnit.MICROSECONDS, TimeUnit.SECONDS);
        waitingOnFreeMemtableSpace = APIMetrics.newTimer("/column_family/metrics/waiting_on_free_memtable/" + cfName,
                factory.createMetricName("WaitingOnFreeMemtableSpace"),
                TimeUnit.MICROSECONDS, TimeUnit.SECONDS);

        trueSnapshotsSize = createColumnFamilyGauge(
                "/column_family/metrics/snapshots_size", "SnapshotsSize");
        rowCacheHitOutOfRange = createColumnFamilyCounter(
                "/column_family/metrics/row_cache_hit_out_of_range",
                "RowCacheHitOutOfRange");
        rowCacheHit = createColumnFamilyCounter(
                "/column_family/metrics/row_cache_hit", "RowCacheHit");
        rowCacheMiss = createColumnFamilyCounter(
                "/column_family/metrics/row_cache_miss", "RowCacheMiss");

        casPrepare = new LatencyMetrics("/column_family/metrics/cas_prepare/"
                + cfName, factory, "CasPrepare");
        casPropose = new LatencyMetrics("/column_family/metrics/cas_propose/"
                + cfName, factory, "CasPropose");
        casCommit = new LatencyMetrics("/column_family/metrics/cas_commit/"
                + cfName, factory, "CasCommit");
        sstablesPerRead = new EstimatedHistogramWrapper("/column_family/metrics/sstables_per_read_histogram/" + cfName);
    }

    /**
     * Release all associated metrics.
     */
    public void release() {
        for (String name : all) {
            allColumnFamilyMetrics.get(name).remove(
                    Metrics.defaultRegistry().allMetrics()
                            .get(factory.createMetricName(name)));
            Metrics.defaultRegistry().removeMetric(
                    factory.createMetricName(name));
        }
        readLatency.release();
        writeLatency.release();
        rangeLatency.release();
        Metrics.defaultRegistry().removeMetric(
                factory.createMetricName("EstimatedRowSizeHistogram"));
        Metrics.defaultRegistry().removeMetric(
                factory.createMetricName("EstimatedColumnCountHistogram"));
        Metrics.defaultRegistry().removeMetric(
                factory.createMetricName("KeyCacheHitRate"));
        Metrics.defaultRegistry().removeMetric(
                factory.createMetricName("CoordinatorReadLatency"));
        Metrics.defaultRegistry().removeMetric(
                factory.createMetricName("CoordinatorScanLatency"));
        Metrics.defaultRegistry().removeMetric(
                factory.createMetricName("WaitingOnFreeMemtableSpace"));
    }

    /**
     * Create a gauge that will be part of a merged version of all column
     * families. The global gauge will merge each CF gauge by adding their
     * values
     */
    protected Gauge<Double> createColumnFamilyGaugeDouble(final String url,
            final String name) {
        Gauge<Double> gauge = new Gauge<Double>() {
            public Double value() {
                return c.getDoubleValue(url + "/" + cfName);
            }
        };
        return createColumnFamilyGauge(url, name, gauge);
    }

    /**
     * Create a gauge that will be part of a merged version of all column
     * families. The global gauge will merge each CF gauge by adding their
     * values
     */
    protected Gauge<Long> createColumnFamilyGauge(final String url, final String name) {
        Gauge<Long> gauge = new Gauge<Long>() {
            public Long value() {
                return (long)c.getDoubleValue(url + "/" + cfName);
            }
        };
        return createColumnFamilyGauge(url, name, gauge);
    }

    /**
     * Create a gauge that will be part of a merged version of all column
     * families. The global gauge will merge each CF gauge by adding their
     * values
     */
    protected Gauge<Integer> createColumnFamilyGaugeInt(final String url,
            final String name) {
        Gauge<Integer> gauge = new Gauge<Integer>() {
            public Integer value() {
                return (int)c.getDoubleValue(url + "/" + cfName);
            }
        };
        return createColumnFamilyGauge(url, name, gauge);
    }

    /**
     * Create a gauge that will be part of a merged version of all column
     * families. The global gauge will merge each CF gauge by adding their
     * values
     */
    protected <T extends Number> Gauge<T> createColumnFamilyGauge(final String url,
            final String name, Gauge<T> gauge) {
        return createColumnFamilyGauge(name, gauge, new Gauge<Long>() {
            public Long value() {
                // This is an optimiztion, call once for all column families
                // instead
                // of iterating over all of them
                return c.getLongValue(url);
            }
        });
    }

    /**
     * Create a gauge that will be part of a merged version of all column
     * families. The global gauge is defined as the globalGauge parameter
     */
    protected <G, T> Gauge<T> createColumnFamilyGauge(String name,
            Gauge<T> gauge, Gauge<G> globalGauge) {
        Gauge<T> cfGauge = APIMetrics.newGauge(factory.createMetricName(name),
                gauge);
        if (register(name, cfGauge)) {
            Metrics.newGauge(globalNameFactory.createMetricName(name),
                    globalGauge);
        }
        return cfGauge;
    }

    /**
     * Creates a counter that will also have a global counter thats the sum of
     * all counters across different column families
     */
    protected Counter createColumnFamilyCounter(final String url, final String name) {
        Counter cfCounter = APIMetrics.newCounter(url + "/" + cfName,
                factory.createMetricName(name));
        if (register(name, cfCounter)) {
            Metrics.newGauge(globalNameFactory.createMetricName(name),
                    new Gauge<Long>() {
                        public Long value() {
                            // This is an optimiztion, call once for all column
                            // families instead
                            // of iterating over all of them
                            return c.getLongValue(url);
                        }
                    });
        }
        return cfCounter;
    }

    /**
     * Create a histogram-like interface that will register both a CF, keyspace
     * and global level histogram and forward any updates to both
     */
    protected ColumnFamilyHistogram createColumnFamilyHistogram(String url,
            String name) {
        Histogram cfHistogram = APIMetrics.newHistogram(url + "/" + cfName,
                factory.createMetricName(name), true);
        register(name, cfHistogram);

        // TBD add keyspace and global histograms
        // keyspaceHistogram,
        // Metrics.newHistogram(globalNameFactory.createMetricName(name),
        // true));
        return new ColumnFamilyHistogram(cfHistogram, null, null);
    }

    /**
     * Registers a metric to be removed when unloading CF.
     *
     * @return true if first time metric with that name has been registered
     */
    private boolean register(String name, Metric metric) {
        boolean ret = allColumnFamilyMetrics.putIfAbsent(name,
                new HashSet<Metric>()) == null;
        allColumnFamilyMetrics.get(name).add(metric);
        all.add(name);
        return ret;
    }

    public long[] getRecentSSTablesPerRead() {
        return recentSSTablesPerRead
                .getBuckets(sstablesPerRead.getBuckets(false));
    }

    public class ColumnFamilyHistogram {
        public final Histogram[] all;
        public final Histogram cf;

        private ColumnFamilyHistogram(Histogram cf, Histogram keyspace,
                Histogram global) {
            this.cf = cf;
            this.all = new Histogram[] { cf, keyspace, global };
        }
    }

    class ColumnFamilyMetricNameFactory implements MetricNameFactory {
        private final String keyspaceName;
        private final String columnFamilyName;
        private final boolean isIndex;

        ColumnFamilyMetricNameFactory(ColumnFamilyStore cfs) {
            this.keyspaceName = cfs.getKeyspace();
            this.columnFamilyName = cfs.getColumnFamilyName();
            isIndex = cfs.isIndex();
        }

        public MetricName createMetricName(String metricName) {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();
            String type = isIndex ? "IndexColumnFamily" : "ColumnFamily";

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=").append(type);
            mbeanName.append(",keyspace=").append(keyspaceName);
            mbeanName.append(",scope=").append(columnFamilyName);
            mbeanName.append(",name=").append(metricName);
            return new MetricName(groupName, type, metricName, keyspaceName
                    + "." + columnFamilyName, mbeanName.toString());
        }
    }

    static class AllColumnFamilyMetricNameFactory implements MetricNameFactory {
        public MetricName createMetricName(String metricName) {
            String groupName = ColumnFamilyMetrics.class.getPackage().getName();
            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            mbeanName.append("type=ColumnFamily");
            mbeanName.append(",name=").append(metricName);
            return new MetricName(groupName, "ColumnFamily", metricName, "all",
                    mbeanName.toString());
        }
    }
}
