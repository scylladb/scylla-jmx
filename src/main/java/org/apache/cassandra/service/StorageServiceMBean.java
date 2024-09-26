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

package org.apache.cassandra.service;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.json.JsonObject;
import javax.management.NotificationEmitter;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.OpenDataException;

public interface StorageServiceMBean extends NotificationEmitter {
    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLiveNodes();

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined by
     * this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getUnreachableNodes();

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getJoiningNodes();

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLeavingNodes();

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getMovingNodes();

    /**
     * Fetch string representations of the tokens for this node.
     *
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens();

    /**
     * Fetch string representations of the tokens for a specified node.
     *
     * @param endpoint
     *            string representation of an node
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens(String endpoint) throws UnknownHostException;

    /**
     * Fetch a string representation of the Cassandra version.
     *
     * @return A string representation of the Cassandra version.
     */
    public String getReleaseVersion();

    /**
     * Fetch a string representation of the current Schema version.
     *
     * @return A string representation of the Schema version.
     */
    public String getSchemaVersion();

    /**
     * Get the list of all data file locations from conf
     *
     * @return String array of all locations
     */
    public String[] getAllDataFileLocations();

    /**
     * Get location of the commit log
     *
     * @return a string path
     */
    public String getCommitLogLocation();

    /**
     * Get location of the saved caches dir
     *
     * @return a string path
     */
    public String getSavedCachesLocation();

    /**
     * Retrieve a map of range to end points that describe the ring topology of
     * a Cassandra cluster.
     *
     * @return mapping of ranges to end points
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(String keyspace);

    /**
     * The same as {@code describeRing(String)} but converts TokenRange to the
     * String for JMX compatibility
     *
     * @param keyspace
     *            The keyspace to fetch information about
     *
     * @return a List of TokenRange(s) converted to String for the given
     *         keyspace
     */
    public List<String> describeRingJMX(String keyspace) throws IOException;

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring
     * topology
     *
     * @param keyspace
     *            the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(String keyspace);

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    public Map<String, String> getTokenToEndpointMap();

    /** Retrieve this hosts unique ID */
    public String getLocalHostId();

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getHostIdMap();

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getEndpointToHostId();

    /** Retrieve the mapping of host ID to endpoint */
    public Map<String, String> getHostIdToEndpoint();

    /** Human-readable load value */
    public String getLoadString();

    /** Human-readable load value. Keys are IP addresses. */
    public Map<String, String> getLoadMap();

    /**
     * Return the generation value for this node.
     *
     * @return generation number
     */
    public int getCurrentGenerationNumber();

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param keyspaceName
     *            keyspace name
     * @param cf
     *            Column family name
     * @param key
     *            - key for which we need to find the endpoint return value -
     *            the endpoint responsible for this key
     */
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf, String key);

    public List<InetAddress> getNaturalEndpoints(String keyspaceName, ByteBuffer key);

    public void checkAndRepairCdcStreams() throws IOException;
    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be
     * specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames
     *            the name of the keyspaces to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... keyspaceNames) throws IOException;

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be specified.
     *
     * @param keyspaceName the keyspace which holds the specified column family
     * @param tableName the table to snapshot
     * @param tag the tag given to the snapshot; may not be null or empty
     */
    default void takeTableSnapshot(String keyspaceName, String tableName, String tag) throws IOException {
        takeColumnFamilySnapshot(keyspaceName, tableName, tag);
    }

    /**
     * Takes the snapshot of a specific column family. A snapshot name must be
     * specified.
     *
     * @param keyspaceName
     *            the keyspace which holds the specified column family
     * @param columnFamilyName
     *            the column family to snapshot
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     */
    public void takeColumnFamilySnapshot(String keyspaceName, String columnFamilyName, String tag) throws IOException;

    /**
     * @deprecated use {@link #takeSnapshot(String tag, Map options, String... entities)} instead.
     */
    @Deprecated
    default public void takeMultipleTableSnapshot(String tag, String... tableList) throws IOException {
        takeMultipleColumnFamilySnapshot(tag, tableList);
    }

    /**
     * Takes the snapshot of a multiple column family from different keyspaces.
     * A snapshot name must be specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param columnFamilyList
     *            list of columnfamily from different keyspace in the form of
     *            ks1.cf1 ks2.cf2
     */
    public void takeMultipleColumnFamilySnapshot(String tag, String... columnFamilyList) throws IOException;

    /**
     * Takes the snapshot of a multiple column family from different keyspaces. A snapshot name must be specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param options
     *            Map of options (skipFlush is the only supported option for now)
     * @param entities
     *            list of keyspaces / tables in the form of empty | ks1 ks2 ... | ks1.cf1,ks2.cf2,...
     */
    public void takeSnapshot(String tag, Map<String, String> options, String... entities) throws IOException;

    /**
     * Remove the snapshot with the given name from the given keyspaces. If no
     * tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames) throws IOException;

    /**
     * Get the details of all the snapshot
     *
     * @return A map of snapshotName to all its details in Tabular form.
     */
    public Map<String, TabularData> getSnapshotDetails();

    /**
     * Get the true size taken by all snapshots across all keyspaces.
     *
     * @return True size taken by all the snapshots.
     */
    public long trueSnapshotsSize();

    /**
     * Forces refresh of values stored in system.size_estimates of all column
     * families.
     */
    public void refreshSizeEstimates() throws ExecutionException;

    /**
     * Forces major compaction of a single keyspace
     */
    public void forceKeyspaceCompaction(boolean splitOutput, boolean offStrategy, String keyspaceName, String... tableNames)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Forces major compaction of a single keyspace.
     */
    public void forceKeyspaceCompaction(boolean offStrategy, String keyspaceName, String... tableNames)
            throws IOException, ExecutionException, InterruptedException;

    @Deprecated
    default public int relocateSSTables(String keyspace, String ... cfnames) throws IOException, ExecutionException, InterruptedException {
        return relocateSSTables(0, keyspace, cfnames);
    }
    default public int relocateSSTables(int jobs, String keyspace, String ... cfnames) throws IOException, ExecutionException, InterruptedException {
        // Node tool op disabled in scylla
        throw new UnsupportedOperationException("relocateSSTables");
    }

    /**
     * Forces major compaction of specified token range in a single keyspace
     */
    public void forceKeyspaceCompactionForTokenRange(String keyspaceName, String startToken, String endToken, String... tableNames) throws IOException, ExecutionException, InterruptedException;

    /**
     * Trigger a cleanup of keys on a single keyspace
     */
    @Deprecated
    public int forceKeyspaceCleanup(String keyspaceName, String... tables)
            throws IOException, ExecutionException, InterruptedException;

    public int forceKeyspaceCleanup(int jobs, String keyspaceName, String... tables)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Scrub (deserialize + reserialize at the latest version, skipping bad rows
     * if any) the given keyspace. If columnFamilies array is empty, all CFs are
     * scrubbed.
     *
     * Scrubbed CFs will be snapshotted first, if disableSnapshot is false
     *
     * scrubMode controls what scrub does when encountering corruption.
     * It replaces skipCorrupted where skipCorrupted is equivalent to scrubMode="SKIP".
     * Can be one of: "ABORT", "SKIP", "SEGREGATE", or "VALIDATE".
     */
    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... tableNames)
            throws IOException, ExecutionException, InterruptedException;

    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, String keyspaceName,
            String... tableNames) throws IOException, ExecutionException, InterruptedException;

    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, int jobs, String keyspaceName,
            String... columnFamilies) throws IOException, ExecutionException, InterruptedException;

    @Deprecated
    public int scrub(boolean disableSnapshot, boolean skipCorrupted, boolean checkData, boolean reinsertOverflowedTTL,
            int jobs, String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException;

    public int scrub(boolean disableSnapshot, String scrubMode, boolean checkData, boolean reinsertOverflowedTTL,
            int jobs, String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Verify (checksums of) the given keyspace. If tableNames array is empty,
     * all CFs are verified.
     *
     * The entire sstable will be read to ensure each cell validates if
     * extendedVerify is true
     */
    public int verify(boolean extendedVerify, String keyspaceName, String... tableNames)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Rewrite all sstables to the latest version. Unlike scrub, it doesn't skip
     * bad rows and do not snapshot sstables first.
     */
    @Deprecated
    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... tableNames)
            throws IOException, ExecutionException, InterruptedException;

    public int upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, int jobs, String... tableNames)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Rewrites all sstables from the given tables to remove deleted data.
     * The tombstone option defines the granularity of the procedure: ROW removes deleted partitions and rows, CELL also removes overwritten or deleted cells.
     */
    default public int garbageCollect(String tombstoneOption, int jobs, String keyspaceName, String... tableNames) throws IOException, ExecutionException, InterruptedException {
        // Node tool op disabled in scylla
        throw new UnsupportedOperationException("garbageCollect");
    }

    /**
     * Flush all memtables for the given column families, or all columnfamilies
     * for the given keyspace if none are explicitly listed.
     *
     * @param keyspaceName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName, String... columnFamilies)
            throws IOException, ExecutionException, InterruptedException;

    /**
     * Invoke repair asynchronously. You can track repair progress by
     * subscribing JMX notification sent from this StorageServiceMBean.
     * Notification format is: type: "repair" userObject: int array of length 2,
     * [0]=command number, [1]=ordinal of ActiveRepairService.Status
     *
     * @param keyspace
     *            Keyspace name to repair. Should not be null.
     * @param options
     *            repair option.
     * @return Repair command number, or 0 if nothing to repair
     */
    public int repairAsync(String keyspace, Map<String, String> options);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)}
     *             instead.
     */
    @Deprecated
    public int forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters,
            Collection<String> hosts, boolean primaryRange, boolean fullRepair, String... tableNames)
            throws IOException;

    /**
     * Invoke repair asynchronously. You can track repair progress by
     * subscribing JMX notification sent from this StorageServiceMBean.
     * Notification format is: type: "repair" userObject: int array of length 2,
     * [0]=command number, [1]=ordinal of ActiveRepairService.Status
     *
     * @deprecated use {@link #repairAsync(String keyspace, Map options)}
     *             instead.
     *
     * @param parallelismDegree
     *            0: sequential, 1: parallel, 2: DC parallel
     * @return Repair command number, or 0 if nothing to repair
     */
    @Deprecated
    public int forceRepairAsync(String keyspace, int parallelismDegree, Collection<String> dataCenters,
            Collection<String> hosts, boolean primaryRange, boolean fullRepair, String... tableNames);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)}
     *             instead.
     */
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential,
            Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... tableNames)
            throws IOException;

    /**
     * Same as forceRepairAsync, but handles a specified range
     *
     * @deprecated use {@link #repairAsync(String keyspace, Map options)}
     *             instead.
     *
     * @param parallelismDegree
     *            0: sequential, 1: parallel, 2: DC parallel
     */
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, int parallelismDegree,
            Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... tableNames);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)}
     *             instead.
     */
    @Deprecated
    public int forceRepairAsync(String keyspace, boolean isSequential, boolean isLocal, boolean primaryRange,
            boolean fullRepair, String... tableNames);

    /**
     * @deprecated use {@link #repairAsync(String keyspace, Map options)}
     *             instead.
     */
    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential,
            boolean isLocal, boolean fullRepair, String... tableNames);

    public void forceTerminateAllRepairSessions();

    /**
     * transfer this node's data to other machines and remove it from service.
     */
    public void decommission() throws InterruptedException;

    /**
     * @param newToken
     *            token to move this node to. This node will unload its data
     *            onto its neighbors, and bootstrap to the new token.
     */
    public void move(String newToken) throws IOException;

    /**
     * removeToken removes token (and all data associated with enpoint that had
     * it) from the ring
     */
    @Deprecated
    public void removeNode(String token);

    public void removeNode(String token, String ignoreNodes);

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus();

    /**
     * Force a remove operation to finish.
     */
    public void forceRemoveCompletion();

    /**
     * set the logging level at runtime<br>
     * <br>
     * If both classQualifer and level are empty/null, it will reload the
     * configuration to reset.<br>
     * If classQualifer is not empty but level is empty/null, it will set the
     * level to null for the defined classQualifer<br>
     * If level cannot be parsed, then the level will be defaulted to DEBUG<br>
     * <br>
     * The logback configuration should have < jmxConfigurator /> set
     *
     * @param classQualifier
     *            The logger's classQualifer
     * @param level
     *            The log level
     * @throws Exception
     *
     * @see ch.qos.logback.classic.Level#toLevel(String)
     */
    public void setLoggingLevel(String classQualifier, String level) throws Exception;

    /** get the runtime logging levels */
    public Map<String, String> getLoggingLevels();

    /**
     * get the operational mode (leaving, joining, normal, decommissioned,
     * client)
     **/
    public String getOperationMode();

    /** Returns whether the storage service is starting or not */
    public boolean isStarting();

    /** get the progress of a drain operation */
    public String getDrainProgress();

    /**
     * makes node unavailable for writes, flushes memtables and replays
     * commitlog.
     */
    public void drain() throws IOException, InterruptedException, ExecutionException;

    /**
     * Truncates (deletes) the given columnFamily from the provided keyspace.
     * Calling truncate results in actual deletion of all data in the cluster
     * under the given columnFamily and it will fail unless all hosts are up.
     * All data in the given column family will be deleted, but its definition
     * will not be affected.
     *
     * @param keyspace
     *            The keyspace to delete from
     * @param columnFamily
     *            The column family to delete data from.
     */
    public void truncate(String keyspace, String columnFamily) throws TimeoutException, IOException;

    /**
     * given a list of tokens (representing the nodes in the cluster), returns a
     * mapping from "token -> %age of cluster owned by that token"
     */
    public Map<InetAddress, Float> getOwnership();

    /**
     * Effective ownership is % of the data each node owns given the keyspace we
     * calculate the percentage using replication factor. If Keyspace == null,
     * this method will try to verify if all the keyspaces in the cluster have
     * the same replication strategies and if yes then we will use the first
     * else a empty Map is returned.
     */
    public Map<InetAddress, Float> effectiveOwnership(String keyspace) throws IllegalStateException;

    public List<String> getKeyspaces();

    public List<String> getNonSystemKeyspaces();

    public Map<String, String> getViewBuildStatuses(String keyspace, String view);

    public List<String> getNonLocalStrategyKeyspaces();

    /**
     * Change endpointsnitch class and dynamic-ness (and dynamic attributes) at
     * runtime
     *
     * @param epSnitchClassName
     *            the canonical path name for a class implementing
     *            IEndpointSnitch
     * @param dynamic
     *            boolean that decides whether dynamicsnitch is used or not
     * @param dynamicUpdateInterval
     *            integer, in ms (default 100)
     * @param dynamicResetInterval
     *            integer, in ms (default 600,000)
     * @param dynamicBadnessThreshold
     *            double, (default 0.0)
     */
    public void updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval,
            Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException;

    /*
     * Update dynamic_snitch_update_interval_in_ms
     */
    default public void setDynamicUpdateInterval(int dynamicUpdateInterval) {
        // afaict not used by nodetool.
        throw new UnsupportedOperationException("setDynamicUpdateInterval");
    }

    /*
     * Get dynamic_snitch_update_interval_in_ms
     */
    default public int getDynamicUpdateInterval() {
        throw new UnsupportedOperationException("getDynamicUpdateInterval");
    }

    // allows a user to forcibly 'kill' a sick node
    public void stopGossiping();

    // allows a user to recover a forcibly 'killed' node
    public void startGossiping();

    // allows a user to see whether gossip is running or not
    public boolean isGossipRunning();

    // allows a user to forcibly completely stop cassandra
    public void stopDaemon();

    // to determine if gossip is disabled
    public boolean isInitialized();

    // allows a user to disable thrift
    public void stopRPCServer();

    // allows a user to reenable thrift
    public void startRPCServer();

    // to determine if thrift is running
    public boolean isRPCServerRunning();

    public void stopNativeTransport();

    public void startNativeTransport();

    public boolean isNativeTransportRunning();

    // allows a node that have been started without joining the ring to join it
    public void joinRing() throws IOException;

    public boolean isJoined();

    default public boolean isDrained() {
        throw new UnsupportedOperationException();
    }

    default public boolean isDraining() {
        throw new UnsupportedOperationException();
    }

    default public void setRpcTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setReadRpcTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getReadRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setRangeRpcTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getRangeRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setWriteRpcTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getWriteRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setCounterWriteRpcTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getCounterWriteRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setCasContentionTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getCasContentionTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setTruncateRpcTimeout(long value) {
        throw new UnsupportedOperationException();
    }

    default public long getTruncateRpcTimeout() {
        throw new UnsupportedOperationException();
    }

    default public void setStreamingSocketTimeout(int value) {
        throw new UnsupportedOperationException();
    }

    default public int getStreamingSocketTimeout() {
        throw new UnsupportedOperationException();
    }

    public void setStreamThroughputMbPerSec(int value);

    public int getStreamThroughputMbPerSec();

    public void setInterDCStreamThroughputMbPerSec(int value);

    public int getInterDCStreamThroughputMbPerSec();

    public int getCompactionThroughputMbPerSec();
    public void setCompactionThroughputMbPerSec(int value);

    default public int getConcurrentCompactors() {
        throw new UnsupportedOperationException();
    }
    default public void setConcurrentCompactors(int value) {
        throw new UnsupportedOperationException();
    }

    public boolean isIncrementalBackupsEnabled();

    public void setIncrementalBackupsEnabled(boolean value);

    /**
     * Initiate a process of streaming data for which we are responsible from
     * other nodes. It is similar to bootstrap except meant to be used on a node
     * which is already in the cluster (typically containing no data) as an
     * alternative to running repair.
     *
     * @param sourceDc
     *            Name of DC from which to select sources for streaming or null
     *            to pick any node
     */
    public void rebuild(String sourceDc);

    /**
     * Same as {@link #rebuild(String)}, but only for specified keyspace and ranges.
     *
     * @param sourceDc Name of DC from which to select sources for streaming or null to pick any node
     * @param keyspace Name of the keyspace which to rebuild or null to rebuild all keyspaces.
     * @param tokens Range of tokens to rebuild or null to rebuild all token ranges. In the format of:
     *               "(start_token_1,end_token_1],(start_token_2,end_token_2],...(start_token_n,end_token_n]"
     */
    public void rebuild(String sourceDc, String keyspace, String tokens, String specificSources);

    /** Starts a bulk load and blocks until it completes. */
    public void bulkLoad(String directory);

    /**
     * Starts a bulk load asynchronously and returns the String representation
     * of the planID for the new streaming session.
     */
    public String bulkLoadAsync(String directory);

    public void rescheduleFailedDeletions();

    /**
     * Load new SSTables to the given keyspace/columnFamily
     *
     * @param ksName
     *            The parent keyspace name
     * @param cfName
     *            The ColumnFamily name where SSTables belong
     * @param isLoadAndStream
     *            Whether or not arbitrary SSTables should be loaded (and streamed to the owning nodes)
     * @param isPrimaryReplicaOnly
     *            Load the sstables and stream to primary replica node that owns the data
     */
    public void loadNewSSTables(String ksName, String cfName, boolean isLoadAndStream, boolean isPrimaryReplicaOnly);

    /**
     * @deprecated use {@link #loadNewSSTables(String ksName, String cfName, boolean isLoadAndStream, boolean isPrimaryReplicaOnly)} instead.
     * This is kept for backward compatibility.
     */
    @Deprecated
    public void loadNewSSTables(String ksName, String cfName);

    /**
     * Return a List of Tokens representing a sample of keys across all
     * ColumnFamilyStores.
     *
     * Note: this should be left as an operation, not an attribute (methods
     * starting with "get") to avoid sending potentially multiple MB of data
     * when accessing this mbean by default. See CASSANDRA-4452.
     *
     * @return set of Tokens as Strings
     */
    public List<String> sampleKeyRange();

    /**
     * rebuild the specified indexes
     */
    public void rebuildSecondaryIndex(String ksName, String cfName, String... idxNames);

    public void resetLocalSchema() throws IOException;

    default public void reloadLocalSchema() {
        throw new UnsupportedOperationException();
    }

    /**
     * Enables/Disables tracing for the whole system. Only thrift requests can
     * start tracing currently.
     *
     * @param probability
     *            ]0,1[ will enable tracing on a partial number of requests with
     *            the provided probability. 0 will disable tracing and 1 will
     *            enable tracing for all requests (which mich severely cripple
     *            the system)
     */
    public void setTraceProbability(double probability);

    /**
     * Returns the configured tracing probability.
     */
    public double getTraceProbability();

    void disableAutoCompaction(String ks, String... columnFamilies) throws IOException;

    void enableAutoCompaction(String ks, String... columnFamilies) throws IOException;

    public void deliverHints(String host) throws UnknownHostException;

    /** Returns the name of the cluster */
    public String getClusterName();

    /** Returns the cluster partitioner */
    public String getPartitionerName();

    /** Returns the threshold for warning of queries with many tombstones */
    public int getTombstoneWarnThreshold();

    /** Sets the threshold for warning queries with many tombstones */
    public void setTombstoneWarnThreshold(int tombstoneDebugThreshold);

    /** Returns the threshold for abandoning queries with many tombstones */
    public int getTombstoneFailureThreshold();

    /** Sets the threshold for abandoning queries with many tombstones */
    public void setTombstoneFailureThreshold(int tombstoneDebugThreshold);

    /** Returns the threshold for rejecting queries due to a large batch size */
    public int getBatchSizeFailureThreshold();

    /** Sets the threshold for rejecting queries due to a large batch size */
    public void setBatchSizeFailureThreshold(int batchSizeDebugThreshold);

    /**
     * Sets the hinted handoff throttle in kb per second, per delivery thread.
     */
    public void setHintedHandoffThrottleInKB(int throttleInKB);

    /**
     * Sets the hinted handoff throttle in kb per second, per delivery thread.
     */
    public boolean resumeBootstrap();
    
    public List<CompositeData> getSSTableInfo(String keyspace, String table);

    public List<CompositeData> getSSTableInfo();
    
    /** retun the system uptime */
    public long getUptime();

    public CompositeData getToppartitions(String sampler, List<String> keyspaceFilters, List<String> tableFilters, int duration, int capacity, int count) throws OpenDataException;
    
    public Map<String, CompositeData> getToppartitions(List<String> samplers, List<String> keyspaceFilters, List<String> tableFilters, int duration, int capacity, int count) throws OpenDataException;
}
