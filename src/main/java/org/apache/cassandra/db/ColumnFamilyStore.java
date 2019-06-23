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

import static java.lang.String.valueOf;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static javax.json.Json.createObjectBuilder;

import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.cassandra.metrics.TableMetrics;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.metrics.MetricsMBean;
import com.sun.jmx.mbeanserver.JmxMBeanServer;
import com.google.common.base.Throwables;

public class ColumnFamilyStore extends MetricsMBean implements ColumnFamilyStoreMBean {
    private static final Logger logger = Logger.getLogger(ColumnFamilyStore.class.getName());
    @SuppressWarnings("unused")
    private final String type;
    private final String keyspace;
    private final String name;
    private static final String[] COUNTER_NAMES = new String[]{"raw", "count", "error", "string"};
    private static final String[] COUNTER_DESCS = new String[]
    { "partition key in raw hex bytes", // Table name and comments match Cassandra, we will use the partition key
      "value of this partition for given sampler",
      "value is within the error bounds plus or minus of this",
      "the partition key turned into a human readable format" };
    private static final CompositeType COUNTER_COMPOSITE_TYPE;
    private static final TabularType COUNTER_TYPE;

    private static final String[] SAMPLER_NAMES = new String[]{"cardinality", "partitions"};
    private static final String[] SAMPLER_DESCS = new String[]
    { "cardinality of partitions",
      "list of counter results" };

    private static final String SAMPLING_RESULTS_NAME = "SAMPLING_RESULTS";
    private static final CompositeType SAMPLING_RESULT;

    public static final String SNAPSHOT_TRUNCATE_PREFIX = "truncated";
    public static final String SNAPSHOT_DROP_PREFIX = "dropped";
    private JsonObject tableSamplerResult = null;

    private Future<JsonObject> futureTableSamperResult = null;
    private ExecutorService service =  null;

    static
    {
        try
        {
            OpenType<?>[] counterTypes = new OpenType[] { SimpleType.STRING, SimpleType.LONG, SimpleType.LONG, SimpleType.STRING };
            COUNTER_COMPOSITE_TYPE = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_NAMES, COUNTER_DESCS, counterTypes);
            COUNTER_TYPE = new TabularType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, COUNTER_COMPOSITE_TYPE, COUNTER_NAMES);

            OpenType<?>[] samplerTypes = new OpenType[] { SimpleType.LONG, COUNTER_TYPE };
            SAMPLING_RESULT = new CompositeType(SAMPLING_RESULTS_NAME, SAMPLING_RESULTS_NAME, SAMPLER_NAMES, SAMPLER_DESCS, samplerTypes);
        } catch (OpenDataException e)
        {
            throw Throwables.propagate(e);
        }
    }

    protected synchronized void startTableSampling(MultivaluedMap<String, String> queryParams) {
        if (futureTableSamperResult != null) {
            return;
        }
        futureTableSamperResult =  service.submit(() -> {
            tableSamplerResult = client.getJsonObj("column_family/toppartitions/" + getCFName(), queryParams);
            return null;
        });
    }

    /*
     * Wait until the action is completed
     * It is safe to call this method multiple times
     */
    public synchronized void waitUntilSamplingCompleted() {
        try {
            if (futureTableSamperResult != null) {
                futureTableSamperResult.get();
                futureTableSamperResult = null;
            }
        } catch (InterruptedException | ExecutionException e) {
            futureTableSamperResult = null;
            throw new RuntimeException("Failed getting table statistics", e);
        }
    }


    public static final Set<String> TYPE_NAMES = new HashSet<>(asList("ColumnFamilies", "IndexTables", "Tables"));

    public void log(String str) {
        logger.finest(str);
    }

    public ColumnFamilyStore(APIClient client, String type, String keyspace, String name) {
        super(client,
                new TableMetrics(keyspace, name, false /* hardcoded for now */));
        this.type = type;
        this.keyspace = keyspace;
        this.name = name;
        service =  Executors.newSingleThreadExecutor();
    }

    public ColumnFamilyStore(APIClient client, ObjectName name) {
        this(client, name.getKeyProperty("type"), name.getKeyProperty("keyspace"), name.getKeyProperty("columnfamily"));
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

    private static ObjectName getName(String type, String keyspace, String name) throws MalformedObjectNameException {
        return new ObjectName(
                "org.apache.cassandra.db:type=" + type + ",keyspace=" + keyspace + ",columnfamily=" + name);
    }

    public static boolean checkRegistration(APIClient client, JmxMBeanServer server) throws MalformedObjectNameException {
        JsonArray mbeans = client.getJsonArray("/column_family/");
        Set<ObjectName> all = new HashSet<ObjectName>();
        for (int i = 0; i < mbeans.size(); i++) {
            JsonObject mbean = mbeans.getJsonObject(i);
            all.add(getName(mbean.getString("type"), mbean.getString("ks"), mbean.getString("cf")));
        }
        return checkRegistration(server, all, n -> TYPE_NAMES.contains(n.getKeyProperty("type")), n -> new ColumnFamilyStore(client, n));
    }

    /**
     * @return the name of the column family
     */
    @Override
    public String getColumnFamilyName() {
        log(" getColumnFamilyName()");
        return name;
    }

    /**
     * force a major compaction of this column family
     */
    public void forceMajorCompaction() throws ExecutionException, InterruptedException {
        log(" forceMajorCompaction() throws ExecutionException, InterruptedException");
        client.post("column_family/major_compaction/" + getCFName());
    }

    /**
     * Gets the minimum number of sstables in queue before compaction kicks off
     */
    @Override
    public int getMinimumCompactionThreshold() {
        log(" getMinimumCompactionThreshold()");
        return client.getIntValue("column_family/minimum_compaction/" + getCFName());
    }

    /**
     * Sets the minimum number of sstables in queue before compaction kicks off
     */
    @Override
    public void setMinimumCompactionThreshold(int threshold) {
        log(" setMinimumCompactionThreshold(int threshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("value", Integer.toString(threshold));
        client.post("column_family/minimum_compaction/" + getCFName(), queryParams);
    }

    /**
     * Gets the maximum number of sstables in queue before compaction kicks off
     */
    @Override
    public int getMaximumCompactionThreshold() {
        log(" getMaximumCompactionThreshold()");
        return client.getIntValue("column_family/maximum_compaction/" + getCFName());
    }

    /**
     * Sets the maximum and maximum number of SSTables in queue before
     * compaction kicks off
     */
    @Override
    public void setCompactionThresholds(int minThreshold, int maxThreshold) {
        log(" setCompactionThresholds(int minThreshold, int maxThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("minimum", Integer.toString(minThreshold));
        queryParams.add("maximum", Integer.toString(maxThreshold));
        client.post("column_family/compaction" + getCFName(), queryParams);
    }

    /**
     * Sets the maximum number of sstables in queue before compaction kicks off
     */
    @Override
    public void setMaximumCompactionThreshold(int threshold) {
        log(" setMaximumCompactionThreshold(int threshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("value", Integer.toString(threshold));
        client.post("column_family/maximum_compaction/" + getCFName(), queryParams);
    }

    /**
     * Sets the compaction strategy by class name
     *
     * @param className
     *            the name of the compaction strategy class
     */
    public void setCompactionStrategyClass(String className) {
        log(" setCompactionStrategyClass(String className)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("class_name", className);
        client.post("column_family/compaction_strategy/" + getCFName(), queryParams);
    }

    /**
     * Gets the compaction strategy class name
     */
    public String getCompactionStrategyClass() {
        log(" getCompactionStrategyClass()");
        return client.getStringValue("column_family/compaction_strategy/" + getCFName());
    }

    /**
     * Get the compression parameters
     */
    @Override
    public Map<String, String> getCompressionParameters() {
        log(" getCompressionParameters()");
        return client.getMapStrValue("column_family/compression_parameters/" + getCFName());
    }

    /**
     * Set the compression parameters
     *
     * @param opts
     *            map of string names to values
     */
    @Override
    public void setCompressionParameters(Map<String, String> opts) {
        log(" setCompressionParameters(Map<String,String> opts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("opts", APIClient.mapToString(opts));
        client.post("column_family/compression_parameters/" + getCFName(), queryParams);
    }

    /**
     * Set new crc check chance
     */
    @Override
    public void setCrcCheckChance(double crcCheckChance) {
        log(" setCrcCheckChance(double crcCheckChance)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("check_chance", Double.toString(crcCheckChance));
        client.post("column_family/crc_check_chance/" + getCFName(), queryParams);
    }

    @Override
    public boolean isAutoCompactionDisabled() {
        log(" isAutoCompactionDisabled()");
        return client.getBooleanValue("column_family/autocompaction/" + getCFName());
    }

    /** Number of tombstoned cells retreived during the last slicequery */
    @Deprecated
    public double getTombstonesPerSlice() {
        log(" getTombstonesPerSlice()");
        return client.getDoubleValue("");
    }

    /** Number of live cells retreived during the last slicequery */
    @Deprecated
    public double getLiveCellsPerSlice() {
        log(" getLiveCellsPerSlice()");
        return client.getDoubleValue("");
    }

    @Override
    public long estimateKeys() {
        log(" estimateKeys()");
        return client.getLongValue("column_family/estimate_keys/" + getCFName());
    }

    /**
     * Returns a list of the names of the built column indexes for current store
     *
     * @return list of the index names
     */
    @Override
    public List<String> getBuiltIndexes() {
        log(" getBuiltIndexes()");
        return client.getListStrValue("column_family/built_indexes/" + getCFName());
    }

    /**
     * Returns a list of filenames that contain the given key on this node
     *
     * @param key
     * @return list of filenames containing the key
     */
    @Override
    public List<String> getSSTablesForKey(String key) {
        log(" getSSTablesForKey(String key)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("key", key);
        return client.getListStrValue("column_family/sstables/by_key/" + getCFName(), queryParams);
    }

    /**
     * Scan through Keyspace/ColumnFamily's data directory determine which
     * SSTables should be loaded and load them
     */
    @Override
    public void loadNewSSTables() {
        log(" loadNewSSTables()");
        client.post("column_family/sstable/" + getCFName());
    }

    /**
     * @return the number of SSTables in L0. Always return 0 if Leveled
     *         compaction is not enabled.
     */
    @Override
    public int getUnleveledSSTables() {
        log(" getUnleveledSSTables()");
        return client.getIntValue("column_family/sstables/unleveled/" + getCFName());
    }

    /**
     * @return sstable count for each level. null unless leveled compaction is
     *         used. array index corresponds to level(int[0] is for level 0,
     *         ...).
     */
    @Override
    public int[] getSSTableCountPerLevel() {
        log(" getSSTableCountPerLevel()");
        int[] res = client.getIntArrValue("column_family/sstables/per_level/" + getCFName());
        if (res.length == 0) {
            // no sstable count
            // should return null
            return null;
        }
        return res;
    }

    /**
     * Get the ratio of droppable tombstones to real columns (and non-droppable
     * tombstones)
     *
     * @return ratio
     */
    @Override
    public double getDroppableTombstoneRatio() {
        log(" getDroppableTombstoneRatio()");
        return client.getDoubleValue("column_family/droppable_ratio/" + getCFName());
    }

    /**
     * @return the size of SSTables in "snapshots" subdirectory which aren't
     *         live anymore
     */
    @Override
    public long trueSnapshotsSize() {
        log(" trueSnapshotsSize()");
        return client.getLongValue("column_family/metrics/snapshots_size/" + getCFName());
    }

    public String getKeyspace() {
        return keyspace;
    }

    @Override
    public String getTableName() {
        log(" getTableName()");
        return name;
    }

    @Override
    public void forceMajorCompaction(boolean splitOutput) throws ExecutionException, InterruptedException {
        log(" forceMajorCompaction(boolean) throws ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.putSingle("value", valueOf(splitOutput));
        client.post("column_family/major_compaction/" + getCFName(), queryParams);
    }

    @Override
    public void setCompactionParametersJson(String options) {
        log(" setCompactionParametersJson");
        JsonReader reader = Json.createReaderFactory(null).createReader(new StringReader(options));
        setCompactionParameters(
                reader.readObject().entrySet().stream().collect(toMap(Map.Entry::getKey, e -> e.toString())));
    }

    @Override
    public String getCompactionParametersJson() {
        log(" getCompactionParametersJson");
        JsonObjectBuilder b = createObjectBuilder();
        getCompactionParameters().forEach(b::add);        
        return b.build().toString();
    }

    @Override
    public void setCompactionParameters(Map<String, String> options) {
        for (Map.Entry<String, String> e : options.entrySet()) {
            // See below
            if ("class".equals(e.getKey())) {
                setCompactionStrategyClass(e.getValue());
            } else {
                throw new IllegalArgumentException(e.getKey());
            }
        }
    }

    @Override
    public Map<String, String> getCompactionParameters() {
        // We only currently support class. Here could have been a call that can 
        // be expanded only on the server side, but that raises controversy. 
        // Lets add some technical debt instead. 
        return Collections.singletonMap("class", getCompactionStrategyClass());
    }

    @Override
    public boolean isCompactionDiskSpaceCheckEnabled() {
        // TODO Auto-generated method stub
        log(" isCompactionDiskSpaceCheckEnabled()");
        return false;
    }

    @Override
    public void compactionDiskSpaceCheck(boolean enable) {
        // TODO Auto-generated method stub
        log(" compactionDiskSpaceCheck()");
    }

    @Override
    public void beginLocalSampling(String sampler_base, int capacity) {
        MultivaluedMap<String, String> queryParams =  new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Integer.toString(capacity));
        if (sampler_base.contains(":")) {
            String[] parts = sampler_base.split(":");
            queryParams.add("duration", parts[1]);
        } else {
            queryParams.add("duration", "10000");
        }
        startTableSampling(queryParams);
        log(" beginLocalSampling()");
    }

    @Override
    public CompositeData finishLocalSampling(String samplerType, int count) throws OpenDataException {
        log(" finishLocalSampling()");

        waitUntilSamplingCompleted();

        TabularDataSupport result = new TabularDataSupport(COUNTER_TYPE);

        JsonArray counters = tableSamplerResult.getJsonArray((samplerType.equalsIgnoreCase("reads")) ? "read" : "write");
        long size = 0;
        if (counters != null) {
            size = (count > counters.size()) ? counters.size() : count;
            for (int i = 0; i < size; i++) {
                JsonObject counter = counters.getJsonObject(i);
                result.put(new CompositeDataSupport(COUNTER_COMPOSITE_TYPE, COUNTER_NAMES,
                        new Object[] { counter.getString("partition"), // raw
                                counter.getJsonNumber("count").longValue(), // count
                                counter.getJsonNumber("error").longValue(), // error
                                counter.getString("partition") })); // string
            }
        }
        //FIXME: size is not the cardinality, a true value needs to be propogated
        return new CompositeDataSupport(SAMPLING_RESULT, SAMPLER_NAMES, new Object[] { size, result });
    }
}
