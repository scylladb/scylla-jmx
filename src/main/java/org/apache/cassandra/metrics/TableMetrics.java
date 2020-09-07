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
package org.apache.cassandra.metrics;

import static com.scylladb.jmx.api.APIClient.getReader;

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.util.Hashtable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.cassandra.db.ColumnFamilyStore;

import com.scylladb.jmx.api.APIClient;

/**
 * Metrics for {@link ColumnFamilyStore}.
 */
public class TableMetrics implements Metrics {
    private final MetricNameFactory factory;
    private final MetricNameFactory aliasFactory;
    private static final MetricNameFactory globalFactory = new AllTableMetricNameFactory("Table");
    private static final MetricNameFactory globalAliasFactory = new AllTableMetricNameFactory("ColumnFamily");
    private static final LatencyMetrics globalLatency[] = new LatencyMetrics[] {
            new LatencyMetrics("Read", compose("read_latency"), globalFactory, globalAliasFactory),
            new LatencyMetrics("Write", compose("read_latency"), globalFactory, globalAliasFactory),
            new LatencyMetrics("Range", compose("read_latency"), globalFactory, globalAliasFactory), };

    private final String cfName;
    private final LatencyMetrics latencyMetrics[];

    public TableMetrics(String keyspace, String columnFamily, boolean isIndex) {
        this.factory = new TableMetricNameFactory(keyspace, columnFamily, isIndex, "Table");
        this.aliasFactory = new TableMetricNameFactory(keyspace, columnFamily, isIndex, "ColumnFamily");
        this.cfName = keyspace + ":" + columnFamily;

        latencyMetrics = new LatencyMetrics[] {
                new LatencyMetrics("Read", compose("read_latency"), cfName, factory, aliasFactory),
                new LatencyMetrics("Write", compose("write_latency"), cfName, factory, aliasFactory),
                new LatencyMetrics("Range", compose("range_latency"), cfName, factory, aliasFactory),

                new LatencyMetrics("CasPrepare", compose("cas_prepare"), cfName, factory, aliasFactory),
                new LatencyMetrics("CasPropose", compose("cas_propose"), cfName, factory, aliasFactory),
                new LatencyMetrics("CasCommit", compose("cas_commit"), cfName, factory, aliasFactory), };
    }

    @Override
    public void register(MetricsRegistry registry) throws MalformedObjectNameException {
        Registry r = new Registry(registry, factory, aliasFactory, cfName);
        registerCommon(r);
        registerLocal(r);
    }

    @Override
    public void registerGlobals(MetricsRegistry registry) throws MalformedObjectNameException {
        Registry r = new Registry(registry, globalFactory, globalAliasFactory, null);
        registerCommon(r);
        for (LatencyMetrics l : globalLatency) {
            l.register(registry);
        }
    }

    private static String compose(String base, String name) {
        String s = "/column_family/metrics/" + base;
        return name != null ? s + "/" + name : s;
    }

    private static String compose(String base) {
        return compose(base, null);
    }

    /**
     * Creates metrics for given {@link ColumnFamilyStore}.
     *
     * @param cfs
     *            ColumnFamilyStore to measure metrics
     */
    static class Registry extends MetricsRegistry {
        @SuppressWarnings("unused")
        private Function<APIClient, Long> newGauge(final String url) {
            return newGauge(Long.class, url);
        }

        public <T> Function<APIClient, T> newGauge(BiFunction<APIClient, String, T> function, String url) {
            return c -> {
                return function.apply(c, url);
            };
        }

        private <T> Function<APIClient, T> newGauge(Class<T> type, final String url) {
            return newGauge(getReader(type), url);
        }

        final MetricNameFactory factory;
        final MetricNameFactory aliasFactory;
        final String cfName;
        final MetricsRegistry other;

        public Registry(MetricsRegistry other, MetricNameFactory factory, MetricNameFactory aliasFactory,
                String cfName) {
            super(other);
            this.other = other;
            this.cfName = cfName;
            this.factory = factory;
            this.aliasFactory = aliasFactory;
        }

        @Override
        public void register(Supplier<MetricMBean> f, ObjectName... objectNames) {
            other.register(f, objectNames);
        }

        public void createTableGauge(String name, String uri) throws MalformedObjectNameException {
            createTableGauge(name, name, uri);
        }

        public void createTableGauge(String name, String alias, String uri) throws MalformedObjectNameException {
            createTableGauge(Long.class, name, alias, uri);
        }

        public <T> void createTableGauge(Class<T> c, String name, String uri) throws MalformedObjectNameException {
            createTableGauge(c, c, name, name, uri);
        }

        public <T> void createTableGauge(Class<T> c, String name, String alias, String uri) throws MalformedObjectNameException {
            createTableGauge(c, name, alias, uri, getReader(c));
        }
        
        public <T> void createTableGauge(Class<T> c, String name, String uri, BiFunction<APIClient, String, T> f)
                throws MalformedObjectNameException {
            createTableGauge(c, name, name, uri, f);
        }

        public <T> void createTableGauge(Class<T> c, String name, String alias, String uri,
                BiFunction<APIClient, String, T> f) throws MalformedObjectNameException {
            register(() -> gauge(newGauge(f, compose(uri, cfName))), factory.createMetricName(name),
                    aliasFactory.createMetricName(alias));
        }

        private static <T> BiFunction<APIClient, String, T> getDummy(Class<T> type) {
            if (type == String.class) {
                return (c, s) -> type.cast("");
            } else if (type == Integer.class) {
                return (c, s) -> type.cast(0);
            } else if (type == Double.class) {
                return (c, s) -> type.cast(0.0);
            } else if (type == Long.class) {
                return (c, s) -> type.cast(0L);
            }
            throw new IllegalArgumentException(type.getName());
        }

        public <T> void createDummyTableGauge(Class<T> c, String name) throws MalformedObjectNameException {
            register(() -> gauge(newGauge(getDummy(c), null)), factory.createMetricName(name),
                    aliasFactory.createMetricName(name));
        }

        public <L, G> void createTableGauge(Class<L> c1, Class<G> c2, String name, String alias, String uri)
                throws MalformedObjectNameException {
            if (cfName != null) { 
                createTableGauge(c1, name, alias, uri, getReader(c1));
            } else { // global case
                createTableGauge(c2, name, alias, uri, getReader(c2));
            }
        }

        public void createTableCounter(String name, String uri) throws MalformedObjectNameException {
            createTableCounter(name, name, uri);
        }

        public void createTableCounter(String name, String alias, String uri) throws MalformedObjectNameException {
            register(() -> counter(compose(uri, cfName)), factory.createMetricName(name),
                    aliasFactory.createMetricName(alias));
        }

        public void createDummyTableCounter(String name) throws MalformedObjectNameException {
            register(() -> counter(null), factory.createMetricName(name),
                    aliasFactory.createMetricName(name));
        }

        public void createTableHistogram(String name, String uri, boolean considerZeros)
                throws MalformedObjectNameException {
            createTableHistogram(name, name, uri, considerZeros);
        }

        public void createTableHistogram(String name, String alias, String uri, boolean considerZeros)
                throws MalformedObjectNameException {
            register(() -> histogram(compose(uri, cfName), considerZeros), factory.createMetricName(name),
                    aliasFactory.createMetricName(alias));
        }

        public void createTimer(String name, String uri) throws MalformedObjectNameException {
            register(() -> timer(compose(uri, cfName)), factory.createMetricName(name));
        }
    }

    private void registerLocal(Registry registry) throws MalformedObjectNameException {
        registry.createTableGauge(long[].class, "EstimatedPartitionSizeHistogram", "EstimatedRowSizeHistogram",
                "estimated_row_size_histogram", APIClient::getEstimatedHistogramAsLongArrValue);
        registry.createTableGauge("EstimatedPartitionCount", "EstimatedRowCount", "estimated_row_count");

        registry.createTableGauge(long[].class, "EstimatedColumnCountHistogram", "estimated_column_count_histogram",
                APIClient::getEstimatedHistogramAsLongArrValue);
        registry.createTableGauge(Double.class, "KeyCacheHitRate", "key_cache_hit_rate");

        registry.createTimer("CoordinatorReadLatency", "coordinator/read");
        registry.createTimer("CoordinatorScanLatency", "coordinator/scan");
        registry.createTimer("WaitingOnFreeMemtableSpace", "waiting_on_free_memtable");

        for (LatencyMetrics l : latencyMetrics) {
            l.register(registry);
        }

        // TODO: implement
        registry.createDummyTableCounter("DroppedMutations");
    }

    private static void registerCommon(Registry registry) throws MalformedObjectNameException {
        registry.createTableGauge("MemtableColumnsCount", "memtable_columns_count");
        registry.createTableGauge("MemtableOnHeapSize", "memtable_on_heap_size");
        registry.createTableGauge("MemtableOffHeapSize", "memtable_off_heap_size");
        registry.createTableGauge("MemtableLiveDataSize", "memtable_live_data_size");
        registry.createTableGauge("AllMemtablesHeapSize", "all_memtables_on_heap_size");
        registry.createTableGauge("AllMemtablesOffHeapSize", "all_memtables_off_heap_size");
        registry.createTableGauge("AllMemtablesLiveDataSize", "all_memtables_live_data_size");

        registry.createTableCounter("MemtableSwitchCount", "memtable_switch_count");

        registry.createTableHistogram("SSTablesPerReadHistogram", "sstables_per_read_histogram", true);
        registry.createTableGauge(Double.class, "CompressionRatio", "compression_ratio");

        registry.createTableCounter("PendingFlushes", "pending_flushes");

        registry.createTableGauge(Integer.class, Long.class, "PendingCompactions", "PendingCompactions",
                "pending_compactions");
        registry.createTableGauge(Integer.class, Long.class, "LiveSSTableCount", "LiveSSTableCount",
                "live_ss_table_count");

        registry.createTableCounter("LiveDiskSpaceUsed", "live_disk_space_used");
        registry.createTableCounter("TotalDiskSpaceUsed", "total_disk_space_used");
        registry.createTableGauge("MinPartitionSize", "MinRowSize", "min_row_size");
        registry.createTableGauge("MaxPartitionSize", "MaxRowSize", "max_row_size");
        registry.createTableGauge("MeanPartitionSize", "MeanRowSize", "mean_row_size");

        registry.createTableGauge("BloomFilterFalsePositives", "bloom_filter_false_positives");
        registry.createTableGauge("RecentBloomFilterFalsePositives", "recent_bloom_filter_false_positives");
        registry.createTableGauge(Double.class, "BloomFilterFalseRatio", "bloom_filter_false_ratio");
        registry.createTableGauge(Double.class, "RecentBloomFilterFalseRatio", "recent_bloom_filter_false_ratio");

        registry.createTableGauge("BloomFilterDiskSpaceUsed", "bloom_filter_disk_space_used");
        registry.createTableGauge("BloomFilterOffHeapMemoryUsed", "bloom_filter_off_heap_memory_used");
        registry.createTableGauge("IndexSummaryOffHeapMemoryUsed", "index_summary_off_heap_memory_used");
        registry.createTableGauge("CompressionMetadataOffHeapMemoryUsed", "compression_metadata_off_heap_memory_used");
        registry.createTableGauge("SpeculativeRetries", "speculative_retries");

        registry.createTableHistogram("TombstoneScannedHistogram", "tombstone_scanned_histogram", false);
        registry.createTableHistogram("LiveScannedHistogram", "live_scanned_histogram", false);
        registry.createTableHistogram("ColUpdateTimeDeltaHistogram", "col_update_time_delta_histogram", false);

        // We do not want to capture view mutation specific metrics for a view
        // They only makes sense to capture on the base table
        // TODO: views
        // if (!cfs.metadata.isView())
        // {
        // viewLockAcquireTime = createTableTimer("ViewLockAcquireTime",
        // cfs.keyspace.metric.viewLockAcquireTime);
        // viewReadTime = createTableTimer("ViewReadTime",
        // cfs.keyspace.metric.viewReadTime);
        // }

        registry.createTableGauge("SnapshotsSize", "snapshots_size");
        registry.createTableCounter("RowCacheHitOutOfRange", "row_cache_hit_out_of_range");
        registry.createTableCounter("RowCacheHit", "row_cache_hit");
        registry.createTableCounter("RowCacheMiss", "row_cache_miss");

        // TODO: implement
        registry.createDummyTableGauge(Double.class, "PercentRepaired");
    }

    static class TableMetricObjectName extends javax.management.ObjectName {
        private final TableMetricStringNameFactory factory;
        private final String metricName;

        public TableMetricObjectName(TableMetricStringNameFactory factory, String metricName) throws MalformedObjectNameException {
            super("");
            this.factory = factory;
            this.metricName = metricName;
        }

        
        @Override
        public boolean isPropertyValuePattern(String property) {
            return false;
        }

        @Override
        public String getCanonicalName() {
            return factory.createMetricStringName(metricName);
        }

        @Override
        public String getDomain() {
            return factory.getDomain();
        }

        @Override
        public String getKeyProperty(String property) {
            if (property == "name") {
                return metricName;
            }
            return factory.getKeyProperty(property);
        }

        @Override
        public Hashtable<String,String> getKeyPropertyList() {
            Hashtable<String, String> res = factory.getKeyPropertyList();
            res.put("name", metricName);
            return res;
        }

        @Override
        public String getKeyPropertyListString() {
            return factory.getKeyPropertyListString(metricName);
        }

        @Override
        public String getCanonicalKeyPropertyListString() {
            return getKeyPropertyListString();
        }

        @Override
        public String toString() {
            return getCanonicalName();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            return getCanonicalName().equals(((ObjectName) o).getCanonicalName());
        }

        @Override
        public int hashCode() {
            return getCanonicalName().hashCode();
        }

        @Override
        public boolean apply(ObjectName name) {
            if (name.isDomainPattern() || name.isPropertyListPattern() || name.isPropertyValuePattern()) {
                return false;
            }
            return getCanonicalName().equals(name.getCanonicalName());
        }


        @Override
        public boolean isPattern() {
            return false;
        }


        @Override
        public boolean isDomainPattern() {
            return false;
        }


        @Override
        public boolean isPropertyPattern() {
            return false;
        }


        @Override
        public boolean isPropertyListPattern() {
            return false;
        }


        @Override
        public boolean isPropertyValuePattern() {
            return false;
        }
        
        /**
         * This type is not really serializable. 
         * Replace it with vanilla objectname.
         */
		private Object writeReplace() throws ObjectStreamException {
			try {
				return new ObjectName(getDomain(), getKeyPropertyList());
			} catch (MalformedObjectNameException e) {
				throw new InvalidObjectException(toString());
			}
		}
    }

    static interface TableMetricStringNameFactory {
        String createMetricStringName(String metricName);
        String getDomain();
        String getKeyProperty(String property);
        Hashtable<String,String> getKeyPropertyList();
        String getKeyPropertyListString(String metricName);
    }

    static class TableMetricNameFactory implements MetricNameFactory, TableMetricStringNameFactory {
        private final String keyspaceName;
        private final String tableName;
        private final boolean isIndex;
        private final String type;

        public TableMetricNameFactory(String keyspaceName, String tableName, boolean isIndex, String type) {
            this.keyspaceName = keyspaceName;
            this.tableName = tableName;
            this.isIndex = isIndex;
            this.type = type;
        }

        private void appendKeyPropertyListString(final StringBuilder sb, final String metricName) {
            String type = isIndex ? "Index" + this.type : this.type;
            // Order matters here - keys have to be sorted
            sb.append("keyspace=").append(keyspaceName);
            sb.append(",name=").append(metricName);
            sb.append(",scope=").append(tableName);
            sb.append(",type=").append(type);
        }

        @Override
        public String createMetricStringName(String metricName) {
            String groupName = TableMetrics.class.getPackage().getName();

            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            appendKeyPropertyListString(mbeanName, metricName);
            return mbeanName.toString();
        }

        @Override
        public String getDomain() {
            return TableMetrics.class.getPackage().getName();
        }

        @Override
        public String getKeyProperty(String property) {
            switch (property) {
                case "keyspace": return keyspaceName;
                case "scope": return tableName;
                case "type": return type;
                default: return null;
            }
        }

        @Override
        public Hashtable<String,String> getKeyPropertyList() {
            Hashtable<String, String> res = new Hashtable<>();
            res.put("keyspace", keyspaceName);
            res.put("scope", tableName);
            res.put("type", type);
            return res;
        }

        @Override
        public String getKeyPropertyListString(String metricName) {
            final StringBuilder sb = new StringBuilder();
            appendKeyPropertyListString(sb, metricName);
            return sb.toString();
        }

        @Override
        public ObjectName createMetricName(String metricName) throws MalformedObjectNameException {
            return new TableMetricObjectName(this, metricName);
        }
    }

    static class AllTableMetricNameFactory implements MetricNameFactory, TableMetricStringNameFactory {
        private final String type;

        public AllTableMetricNameFactory(String type) {
            this.type = type;
        }

        private void appendKeyPropertyListString(final StringBuilder sb, final String metricName) {
            // Order matters here - keys have to be sorted
            sb.append("name=").append(metricName);
            sb.append(",type=" + type);
        }

        @Override
        public String createMetricStringName(String metricName) {
            String groupName = TableMetrics.class.getPackage().getName();
            StringBuilder mbeanName = new StringBuilder();
            mbeanName.append(groupName).append(":");
            appendKeyPropertyListString(mbeanName, metricName);
            return mbeanName.toString();
        }

        @Override
        public String getDomain() {
            return TableMetrics.class.getPackage().getName();
        }

        @Override
        public String getKeyProperty(String property) {
            switch (property) {
                case "type": return type;
                default: return null;
            }
        }

        @Override
        public Hashtable<String,String> getKeyPropertyList() {
            Hashtable<String, String> res = new Hashtable<>();
            res.put("type", type);
            return res;
        }

        @Override
        public String getKeyPropertyListString(String metricName) {
            final StringBuilder sb = new StringBuilder();
            appendKeyPropertyListString(sb, metricName);
            return sb.toString();
        }

        @Override
        public ObjectName createMetricName(String metricName) throws MalformedObjectNameException {
            return new TableMetricObjectName(this, metricName);
        }
    }

    public enum Sampler {
        READS, WRITES
    }
}
