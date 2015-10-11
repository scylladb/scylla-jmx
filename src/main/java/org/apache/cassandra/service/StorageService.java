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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.*;
import javax.management.openmbean.TabularData;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.repair.RepairParallelism;
import com.cloudius.urchin.api.APIClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * This abstraction contains the token/identifier of this node on the identifier
 * space. This token gets gossiped around. This class will also maintain
 * histograms of the load information of other nodes in the cluster.
 */
public class StorageService extends NotificationBroadcasterSupport
        implements StorageServiceMBean {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(StorageService.class.getName());

    private APIClient c = new APIClient();
    private static Timer timer = new Timer("Storage Service Repair");
    private StorageMetrics metrics = new StorageMetrics();

    public static final StorageService instance = new StorageService();

    public static StorageService getInstance() {
        return instance;
    }

    public static enum RepairStatus
    {
        STARTED, SESSION_SUCCESS, SESSION_FAILED, FINISHED
    }

    /* JMX notification serial number counter */
    private final AtomicLong notificationSerialNumber = new AtomicLong();

    private final ObjectName jmxObjectName;

    public StorageService() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            jmxObjectName = new ObjectName(
                    "org.apache.cassandra.db:type=StorageService");
            mbs.registerMBean(this, jmxObjectName);
            // mbs.registerMBean(StreamManager.instance, new ObjectName(
            // StreamManager.OBJECT_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void log(String str) {
        System.out.println(str);
        logger.info(str);
    }

    /**
     * Retrieve the list of live nodes in the cluster, where "liveness" is
     * determined by the failure detector of the node being queried.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLiveNodes() {
        log(" getLiveNodes()");
        return c.getListStrValue("/gossiper/endpoint/live");
    }

    /**
     * Retrieve the list of unreachable nodes in the cluster, as determined by
     * this node's failure detector.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getUnreachableNodes() {
        log(" getUnreachableNodes()");
        return c.getListStrValue("/gossiper/endpoint/down");
    }

    /**
     * Retrieve the list of nodes currently bootstrapping into the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getJoiningNodes() {
        log(" getJoiningNodes()");
        return c.getListStrValue("/storage_service/nodes/joining");
    }

    /**
     * Retrieve the list of nodes currently leaving the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getLeavingNodes() {
        log(" getLeavingNodes()");
        return c.getListStrValue("/storage_service/nodes/leaving");
    }

    /**
     * Retrieve the list of nodes currently moving in the ring.
     *
     * @return set of IP addresses, as Strings
     */
    public List<String> getMovingNodes() {
        log(" getMovingNodes()");
        return c.getListStrValue("/storage_service/nodes/moving");
    }

    /**
     * Fetch string representations of the tokens for this node.
     *
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens() {
        log(" getTokens()");
        return c.getListStrValue("/storage_service/tokens/");
    }

    /**
     * Fetch string representations of the tokens for a specified node.
     *
     * @param endpoint
     *            string representation of an node
     * @return a collection of tokens formatted as strings
     */
    public List<String> getTokens(String endpoint) throws UnknownHostException {
        log(" getTokens(String endpoint) throws UnknownHostException");
        return c.getListStrValue("/storage_service/tokens/" + endpoint);
    }

    /**
     * Fetch a string representation of the Cassandra version.
     *
     * @return A string representation of the Cassandra version.
     */
    public String getReleaseVersion() {
        log(" getReleaseVersion()");
        return c.getStringValue("/storage_service/release_version");
    }

    /**
     * Fetch a string representation of the current Schema version.
     *
     * @return A string representation of the Schema version.
     */
    public String getSchemaVersion() {
        log(" getSchemaVersion()");
        return c.getStringValue("/storage_service/schema_version");
    }

    /**
     * Get the list of all data file locations from conf
     *
     * @return String array of all locations
     */
    public String[] getAllDataFileLocations() {
        log(" getAllDataFileLocations()");
        return c.getStringArrValue("/storage_service/data_file/locations");
    }

    /**
     * Get location of the commit log
     *
     * @return a string path
     */
    public String getCommitLogLocation() {
        log(" getCommitLogLocation()");
        return c.getStringValue("/storage_service/commitlog");
    }

    /**
     * Get location of the saved caches dir
     *
     * @return a string path
     */
    public String getSavedCachesLocation() {
        log(" getSavedCachesLocation()");
        return c.getStringValue("/storage_service/saved_caches/location");
    }

    /**
     * Retrieve a map of range to end points that describe the ring topology of
     * a Cassandra cluster.
     *
     * @return mapping of ranges to end points
     */
    public Map<List<String>, List<String>> getRangeToEndpointMap(
            String keyspace) {
        log(" getRangeToEndpointMap(String keyspace)");
        return c.getMapListStrValue("/storage_service/range/" + keyspace);
    }

    /**
     * Retrieve a map of range to rpc addresses that describe the ring topology
     * of a Cassandra cluster.
     *
     * @return mapping of ranges to rpc addresses
     */
    public Map<List<String>, List<String>> getRangeToRpcaddressMap(
            String keyspace) {
        log(" getRangeToRpcaddressMap(String keyspace)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("rpc", "true");
        return c.getMapListStrValue("/storage_service/range/" + keyspace,
                queryParams);
    }

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
    public List<String> describeRingJMX(String keyspace) throws IOException {
        log(" describeRingJMX(String keyspace) throws IOException");
        return c.getListStrValue("/storage_service/describe_ring/" + keyspace);
    }

    /**
     * Retrieve a map of pending ranges to endpoints that describe the ring
     * topology
     *
     * @param keyspace
     *            the keyspace to get the pending range map for.
     * @return a map of pending ranges to endpoints
     */
    public Map<List<String>, List<String>> getPendingRangeToEndpointMap(
            String keyspace) {
        log(" getPendingRangeToEndpointMap(String keyspace)");
        return c.getMapListStrValue(
                "/storage_service/pending_range/" + keyspace);
    }

    /**
     * Retrieve a map of tokens to endpoints, including the bootstrapping ones.
     *
     * @return a map of tokens to endpoints in ascending order
     */
    public Map<String, String> getTokenToEndpointMap() {
        log(" getTokenToEndpointMap()");
        return c.getMapStrValue("/storage_service/tokens_endpoint");
    }

    /** Retrieve this hosts unique ID */
    public String getLocalHostId() {
        log(" getLocalHostId()");
        return c.getStringValue("/storage_service/hostid/local");
    }

    /** Retrieve the mapping of endpoint to host ID */
    public Map<String, String> getHostIdMap() {
        log(" getHostIdMap()");
        return c.getMapStrValue("/storage_service/host_id");
    }

    /**
     * Numeric load value.
     *
     * @see org.apache.cassandra.metrics.StorageMetrics#load
     */
    @Deprecated
    public double getLoad() {
        log(" getLoad()");
        return c.getDoubleValue("/storage_service/load");
    }

    /** Human-readable load value */
    public String getLoadString() {
        log(" getLoadString()");
        return String.valueOf(getLoad());
    }

    /** Human-readable load value. Keys are IP addresses. */
    public Map<String, String> getLoadMap() {
        log(" getLoadMap()");
        return c.getMapStrValue("/storage_service/load_map");
    }

    /**
     * Return the generation value for this node.
     *
     * @return generation number
     */
    public int getCurrentGenerationNumber() {
        log(" getCurrentGenerationNumber()");
        return c.getIntValue("/storage_service/generation_number");
    }

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
    public List<InetAddress> getNaturalEndpoints(String keyspaceName, String cf,
            String key) {
        log(" getNaturalEndpoints(String keyspaceName, String cf, String key)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("cf", cf);
        queryParams.add("key", key);
        return c.getListInetAddressValue(
                "/storage_service/natural_endpoints/" + keyspaceName,
                queryParams);
    }

    public List<InetAddress> getNaturalEndpoints(String keyspaceName,
            ByteBuffer key) {
        log(" getNaturalEndpoints(String keyspaceName, ByteBuffer key)");
        return c.getListInetAddressValue("");
    }

    /**
     * Takes the snapshot for the given keyspaces. A snapshot name must be
     * specified.
     *
     * @param tag
     *            the tag given to the snapshot; may not be null or empty
     * @param keyspaceNames
     *            the name of the keyspaces to snapshot; empty means "all."
     */
    public void takeSnapshot(String tag, String... keyspaceNames)
            throws IOException {
        log(" takeSnapshot(String tag, String... keyspaceNames) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "tag", tag);
        APIClient.set_query_param(queryParams, "kn",
                APIClient.join(keyspaceNames));
        c.post("/storage_service/snapshot", queryParams);
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
    public void takeColumnFamilySnapshot(String keyspaceName,
            String columnFamilyName, String tag) throws IOException {
        log(" takeColumnFamilySnapshot(String keyspaceName, String columnFamilyName, String tag) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        if (keyspaceName == null)
            throw new IOException("You must supply a keyspace name");
        if (columnFamilyName == null)
            throw new IOException("You must supply a table name");
        if (tag == null || tag.equals(""))
            throw new IOException("You must supply a snapshot name.");
        queryParams.add("tag", tag);
        queryParams.add("kn", keyspaceName);
        queryParams.add("cf", columnFamilyName);
        c.post("/storage_service/snapshots", queryParams);
    }

    /**
     * Remove the snapshot with the given name from the given keyspaces. If no
     * tag is specified we will remove all snapshots.
     */
    public void clearSnapshot(String tag, String... keyspaceNames)
            throws IOException {
        log(" clearSnapshot(String tag, String... keyspaceNames) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "tag", tag);
        APIClient.set_query_param(queryParams, "kn",
                APIClient.join(keyspaceNames));
        c.delete("/storage_service/snapshots", queryParams);
    }

    /**
     * Get the details of all the snapshot
     *
     * @return A map of snapshotName to all its details in Tabular form.
     */
    public Map<String, TabularData> getSnapshotDetails() {
        log(" getSnapshotDetails()");
        return c.getMapStringSnapshotTabularDataValue(
                "/storage_service/snapshots", null);
    }

    /**
     * Get the true size taken by all snapshots across all keyspaces.
     *
     * @return True size taken by all the snapshots.
     */
    public long trueSnapshotsSize() {
        log(" trueSnapshotsSize()");
        return c.getLongValue("/storage_service/snapshots/size/true");
    }

    /**
     * Forces major compaction of a single keyspace
     */
    public void forceKeyspaceCompaction(String keyspaceName,
            String... columnFamilies) throws IOException, ExecutionException,
                    InterruptedException {
        log(" forceKeyspaceCompaction(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        c.post("/storage_service/keyspace_compaction/" + keyspaceName,
                queryParams);
    }

    /**
     * Trigger a cleanup of keys on a single keyspace
     */
    public int forceKeyspaceCleanup(String keyspaceName,
            String... columnFamilies) throws IOException, ExecutionException,
                    InterruptedException {
        log(" forceKeyspaceCleanup(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        return c.postInt("/storage_service/keyspace_cleanup/" + keyspaceName,
                queryParams);
    }

    /**
     * Scrub (deserialize + reserialize at the latest version, skipping bad rows
     * if any) the given keyspace. If columnFamilies array is empty, all CFs are
     * scrubbed.
     *
     * Scrubbed CFs will be snapshotted first, if disableSnapshot is false
     */
    public int scrub(boolean disableSnapshot, boolean skipCorrupted,
            String keyspaceName, String... columnFamilies) throws IOException,
                    ExecutionException, InterruptedException {
        log(" scrub(boolean disableSnapshot, boolean skipCorrupted, String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_bool_query_param(queryParams, "disable_snapshot",
                disableSnapshot);
        APIClient.set_bool_query_param(queryParams, "skip_corrupted",
                skipCorrupted);
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        return c.getIntValue("/storage_service/keyspace_scrub/" + keyspaceName);
    }

    /**
     * Rewrite all sstables to the latest version. Unlike scrub, it doesn't skip
     * bad rows and do not snapshot sstables first.
     */
    public int upgradeSSTables(String keyspaceName,
            boolean excludeCurrentVersion, String... columnFamilies)
                    throws IOException, ExecutionException,
                    InterruptedException {
        log(" upgradeSSTables(String keyspaceName, boolean excludeCurrentVersion, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_bool_query_param(queryParams, "exclude_current_version",
                excludeCurrentVersion);
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        return c.getIntValue(
                "/storage_service/keyspace_upgrade_sstables/" + keyspaceName);
    }

    /**
     * Flush all memtables for the given column families, or all columnfamilies
     * for the given keyspace if none are explicitly listed.
     *
     * @param keyspaceName
     * @param columnFamilies
     * @throws IOException
     */
    public void forceKeyspaceFlush(String keyspaceName,
            String... columnFamilies) throws IOException, ExecutionException,
                    InterruptedException {
        log(" forceKeyspaceFlush(String keyspaceName, String... columnFamilies) throws IOException, ExecutionException, InterruptedException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        c.post("/storage_service/keyspace_flush/" + keyspaceName, queryParams);
    }

    class CheckRepair extends TimerTask {
        private APIClient c = new APIClient();
        int id;
        String keyspace;
        String message;
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        int cmd;
        public CheckRepair(int id, String keyspace) {
            this.id = id;
            this.keyspace = keyspace;
            APIClient.set_query_param(queryParams, "id", Integer.toString(id));
            message = String.format("Repair session %d ", id);
            // The returned id is the command number
            this.cmd = id;
        }
        @Override
        public void run() {
            String status = c.getStringValue("/storage_service/repair_async/" + keyspace, queryParams);
            if (!status.equals("RUNNING")) {
                cancel();
                if (!status.equals("SUCCESSFUL")) {
                    sendNotification("repair", message + "failed", new int[]{cmd, RepairStatus.SESSION_FAILED.ordinal()});
                }
                sendNotification("repair", message + "finished", new int[]{cmd, RepairStatus.FINISHED.ordinal()});
            }
        }

    }

    /**
     * Sends JMX notification to subscribers.
     *
     * @param type Message type
     * @param message Message itself
     * @param userObject Arbitrary object to attach to notification
     */
    public void sendNotification(String type, String message, Object userObject)
    {
        Notification jmxNotification = new Notification(type, jmxObjectName, notificationSerialNumber.incrementAndGet(), message);
        jmxNotification.setUserData(userObject);
        sendNotification(jmxNotification);
    }

    public String getRepairMessage(final int cmd,
            final String keyspace,
            final int ranges_size,
            final RepairParallelism parallelismDegree,
            final boolean fullRepair) {
        return String.format("Starting repair command #%d, repairing %d ranges for keyspace %s (parallelism=%s, full=%b)",
                cmd, ranges_size, keyspace, parallelismDegree, fullRepair);
    }

    /**
     *
     * @param repair
     */
    public int waitAndNotifyRepair(int cmd, String keyspace, String message) {
        logger.info(message);
        sendNotification("repair", message, new int[]{cmd, RepairStatus.STARTED.ordinal()});
        TimerTask taskToExecute = new CheckRepair(cmd, keyspace);
        timer.schedule(taskToExecute, 100, 1000);
        return cmd;
    }

    /**
     * Invoke repair asynchronously. You can track repair progress by
     * subscribing JMX notification sent from this StorageServiceMBean.
     * Notification format is: type: "repair" userObject: int array of length 2,
     * [0]=command number, [1]=ordinal of AntiEntropyService.Status
     *
     * @param keyspace
     *            Keyspace name to repair. Should not be null.
     * @param options
     *            repair option.
     * @return Repair command number, or 0 if nothing to repair
     */
    public int repairAsync(String keyspace, Map<String, String> options) {
        log(" repairAsync(String keyspace, Map<String, String> options)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        String opts = "";
        for (String op : options.keySet()) {
            if (!opts.equals("")) {
                opts = opts + ",";
            }
            opts = opts + op + "=" + options.get(op);
        }
        APIClient.set_query_param(queryParams, "options", opts);
        int cmd = c.postInt("/storage_service/repair_async/" + keyspace);
        waitAndNotifyRepair(cmd, keyspace, getRepairMessage(cmd, keyspace, 1, RepairParallelism.SEQUENTIAL, true));
        return cmd;
    }

    @Override
    public int forceRepairAsync(String keyspace, boolean isSequential,
            Collection<String> dataCenters, Collection<String> hosts,
            boolean primaryRange, boolean repairedAt, String... columnFamilies)
                    throws IOException {
        log(" forceRepairAsync(String keyspace, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts,  boolean primaryRange, boolean repairedAt, String... columnFamilies) throws IOException");
        Map<String, String> options = new HashMap<String, String>();
        return repairAsync(keyspace, options);
    }

    public int forceRepairAsync(String keyspace) {
        Map<String, String> options = new HashMap<String, String>();
        return repairAsync(keyspace, options);
    }

    public int forceRepairRangeAsync(String beginToken, String endToken,
            String keyspaceName, boolean isSequential,
            Collection<String> dataCenters, Collection<String> hosts,
            boolean repairedAt, String... columnFamilies) throws IOException {
        log(" forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, Collection<String> dataCenters, Collection<String> hosts, boolean repairedAt, String... columnFamilies) throws IOException");
        return c.getIntValue("");
    }

    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken,
            String keyspaceName, RepairParallelism parallelismDegree,
            Collection<String> dataCenters, Collection<String> hosts,
            boolean fullRepair, String... columnFamilies) {
        log(" forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, RepairParallelism parallelismDegree, Collection<String> dataCenters, Collection<String> hosts, boolean fullRepair, String... columnFamilies)");
        return c.getIntValue("");
    }

    @Override
    public int forceRepairAsync(String keyspace, boolean isSequential,
            boolean isLocal, boolean primaryRange, boolean fullRepair,
            String... columnFamilies) {
        log(" forceRepairAsync(String keyspace, boolean isSequential, boolean isLocal, boolean primaryRange, boolean fullRepair, String... columnFamilies)");
        Map<String, String> options = new HashMap<String, String>();
        return repairAsync(keyspace, options);
    }

    @Deprecated
    public int forceRepairRangeAsync(String beginToken, String endToken,
            String keyspaceName, boolean isSequential, boolean isLocal,
            boolean repairedAt, String... columnFamilies) {
        log(" forceRepairRangeAsync(String beginToken, String endToken, String keyspaceName, boolean isSequential, boolean isLocal, boolean repairedAt, String... columnFamilies)");
        return c.getIntValue("");
    }

    public void forceTerminateAllRepairSessions() {
        log(" forceTerminateAllRepairSessions()");
        c.post("/storage_service/force_terminate");
    }

    /**
     * transfer this node's data to other machines and remove it from service.
     */
    public void decommission() throws InterruptedException {
        log(" decommission() throws InterruptedException");
        c.post("/storage_service/decommission");
    }

    /**
     * @param newToken
     *            token to move this node to. This node will unload its data
     *            onto its neighbors, and bootstrap to the new token.
     */
    public void move(String newToken) throws IOException {
        log(" move(String newToken) throws IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "new_token", newToken);
        c.post("/storage_service/move");
    }

    /**
     * removeToken removes token (and all data associated with enpoint that had
     * it) from the ring
     */
    public void removeNode(String token) {
        log(" removeNode(String token)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "new_token", token);
        c.post("/storage_service/remove_node", queryParams);
    }

    /**
     * Get the status of a token removal.
     */
    public String getRemovalStatus() {
        log(" getRemovalStatus()");
        return c.getStringValue("/storage_service/removal_status");
    }

    /**
     * Force a remove operation to finish.
     */
    public void forceRemoveCompletion() {
        log(" forceRemoveCompletion()");
        c.post("/storage_service/force_remove_completion");
    }

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
    public void setLoggingLevel(String classQualifier, String level)
            throws Exception {
        log(" setLoggingLevel(String classQualifier, String level) throws Exception");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "level", level);
        APIClient.set_query_param(queryParams, "class_qualifier",
                classQualifier);
        c.post("/storage_service/logging_level", queryParams);
    }

    /** get the runtime logging levels */
    public Map<String, String> getLoggingLevels() {
        log(" getLoggingLevels()");
        return c.getMapStrValue("/storage_service/logging_level");
    }

    /**
     * get the operational mode (leaving, joining, normal, decommissioned,
     * client)
     **/
    public String getOperationMode() {
        log(" getOperationMode()");
        return c.getStringValue("/storage_service/operation_mode");
    }

    /** Returns whether the storage service is starting or not */
    public boolean isStarting() {
        log(" isStarting()");
        return c.getBooleanValue("/storage_service/is_starting");
    }

    /** get the progress of a drain operation */
    public String getDrainProgress() {
        log(" getDrainProgress()");
        return c.getStringValue("/storage_service/drain");
    }

    /**
     * makes node unavailable for writes, flushes memtables and replays
     * commitlog.
     */
    public void drain()
            throws IOException, InterruptedException, ExecutionException {
        log(" drain() throws IOException, InterruptedException, ExecutionException");
        c.post("/storage_service/drain");
    }

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
    public void truncate(String keyspace, String columnFamily)
            throws TimeoutException, IOException {
        log(" truncate(String keyspace, String columnFamily)throws TimeoutException, IOException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "cf", columnFamily);
        c.post("/storage_service/truncate/" + keyspace, queryParams);
    }

    /**
     * given a list of tokens (representing the nodes in the cluster), returns a
     * mapping from "token -> %age of cluster owned by that token"
     */
    public Map<InetAddress, Float> getOwnership() {
        log(" getOwnership()");
        return c.getMapInetAddressFloatValue("/storage_service/ownership/");
    }

    /**
     * Effective ownership is % of the data each node owns given the keyspace we
     * calculate the percentage using replication factor. If Keyspace == null,
     * this method will try to verify if all the keyspaces in the cluster have
     * the same replication strategies and if yes then we will use the first
     * else a empty Map is returned.
     */
    public Map<InetAddress, Float> effectiveOwnership(String keyspace)
            throws IllegalStateException {
        log(" effectiveOwnership(String keyspace) throws IllegalStateException");
        try {
            return c.getMapInetAddressFloatValue("/storage_service/ownership/" + keyspace);
        } catch (Exception e) {
            throw new IllegalStateException("Non-system keyspaces don't have the same replication settings, effective ownership information is meaningless");
        }
    }

    public List<String> getKeyspaces() {
        log(" getKeyspaces()");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("non_system", "true");
        return c.getListStrValue("/storage_service/keyspaces", queryParams);
    }

    public List<String> getNonSystemKeyspaces() {
        log(" getNonSystemKeyspaces()");
        return c.getListStrValue("/storage_service/keyspaces");
    }

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
    public void updateSnitch(String epSnitchClassName, Boolean dynamic,
            Integer dynamicUpdateInterval, Integer dynamicResetInterval,
            Double dynamicBadnessThreshold) throws ClassNotFoundException {
        log(" updateSnitch(String epSnitchClassName, Boolean dynamic, Integer dynamicUpdateInterval, Integer dynamicResetInterval, Double dynamicBadnessThreshold) throws ClassNotFoundException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_bool_query_param(queryParams, "dynamic", dynamic);
        APIClient.set_query_param(queryParams, "epSnitchClassName",
                epSnitchClassName);
        if (dynamicUpdateInterval != null) {
            queryParams.add("dynamic_update_interval",
                    dynamicUpdateInterval.toString());
        }
        if (dynamicResetInterval != null) {
            queryParams.add("dynamic_reset_interval",
                    dynamicResetInterval.toString());
        }
        if (dynamicBadnessThreshold != null) {
            queryParams.add("dynamic_badness_threshold",
                    dynamicBadnessThreshold.toString());
        }
        c.post("/storage_service/update_snitch", queryParams);
    }

    // allows a user to forcibly 'kill' a sick node
    public void stopGossiping() {
        log(" stopGossiping()");
        c.delete("/storage_service/gossiping");
    }

    // allows a user to recover a forcibly 'killed' node
    public void startGossiping() {
        log(" startGossiping()");
        c.post("/storage_service/gossiping");
    }

    // allows a user to see whether gossip is running or not
    public boolean isGossipRunning() {
        log(" isGossipRunning()");
        return c.getBooleanValue("/storage_service/gossiping");
    }

    // allows a user to forcibly completely stop cassandra
    public void stopDaemon() {
        log(" stopDaemon()");
        c.post("/storage_service/stop_daemon");
    }

    // to determine if gossip is disabled
    public boolean isInitialized() {
        log(" isInitialized()");
        return c.getBooleanValue("/storage_service/is_initialized");
    }

    // allows a user to disable thrift
    public void stopRPCServer() {
        log(" stopRPCServer()");
        c.delete("/storage_service/rpc_server");
    }

    // allows a user to reenable thrift
    public void startRPCServer() {
        log(" startRPCServer()");
        c.post("/storage_service/rpc_server");
    }

    // to determine if thrift is running
    public boolean isRPCServerRunning() {
        log(" isRPCServerRunning()");
        return c.getBooleanValue("/storage_service/rpc_server");
    }

    public void stopNativeTransport() {
        log(" stopNativeTransport()");
        c.delete("/storage_service/native_transport");
    }

    public void startNativeTransport() {
        log(" startNativeTransport()");
        c.post("/storage_service/native_transport");
    }

    public boolean isNativeTransportRunning() {
        log(" isNativeTransportRunning()");
        return c.getBooleanValue("/storage_service/native_transport");
    }

    // allows a node that have been started without joining the ring to join it
    public void joinRing() throws IOException {
        log(" joinRing() throws IOException");
        c.post("/storage_service/join_ring");
    }

    public boolean isJoined() {
        log(" isJoined()");
        return c.getBooleanValue("/storage_service/join_ring");
    }

    @Deprecated
    public int getExceptionCount() {
        log(" getExceptionCount()");
        return c.getIntValue("");
    }

    public void setStreamThroughputMbPerSec(int value) {
        log(" setStreamThroughputMbPerSec(int value)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("value", Integer.toString(value));
        c.post("/storage_service/stream_throughput", queryParams);

    }

    public int getStreamThroughputMbPerSec() {
        log(" getStreamThroughputMbPerSec()");
        return c.getIntValue("/storage_service/stream_throughput");
    }

    public int getCompactionThroughputMbPerSec() {
        log(" getCompactionThroughputMbPerSec()");
        return c.getIntValue("/storage_service/compaction_throughput");
    }

    public void setCompactionThroughputMbPerSec(int value) {
        log(" setCompactionThroughputMbPerSec(int value)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("value", Integer.toString(value));
        c.post("/storage_service/compaction_throughput", queryParams);
    }

    public boolean isIncrementalBackupsEnabled() {
        log(" isIncrementalBackupsEnabled()");
        return c.getBooleanValue("/storage_service/incremental_backups");
    }

    public void setIncrementalBackupsEnabled(boolean value) {
        log(" setIncrementalBackupsEnabled(boolean value)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("value", Boolean.toString(value));
        c.post("/storage_service/incremental_backups", queryParams);
    }

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
    public void rebuild(String sourceDc) {
        log(" rebuild(String sourceDc)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "value", sourceDc);
        c.post("/storage_service/rebuild", queryParams);
    }

    /** Starts a bulk load and blocks until it completes. */
    public void bulkLoad(String directory) {
        log(" bulkLoad(String directory)");
        c.post("/storage_service/bulk_load/" + directory);
    }

    /**
     * Starts a bulk load asynchronously and returns the String representation
     * of the planID for the new streaming session.
     */
    public String bulkLoadAsync(String directory) {
        log(" bulkLoadAsync(String directory)");
        return c.getStringValue(
                "/storage_service/bulk_load_async/" + directory);
    }

    public void rescheduleFailedDeletions() {
        log(" rescheduleFailedDeletions()");
        c.post("/storage_service/reschedule_failed_deletions");
    }

    /**
     * Load new SSTables to the given keyspace/columnFamily
     *
     * @param ksName
     *            The parent keyspace name
     * @param cfName
     *            The ColumnFamily name where SSTables belong
     */
    public void loadNewSSTables(String ksName, String cfName) {
        log(" loadNewSSTables(String ksName, String cfName)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("cf", cfName);
        c.post("/storage_service/sstables/" + ksName, queryParams);
    }

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
    public List<String> sampleKeyRange() {
        log(" sampleKeyRange()");
        return c.getListStrValue("/storage_service/sample_key_range");
    }

    /**
     * rebuild the specified indexes
     */
    public void rebuildSecondaryIndex(String ksName, String cfName,
            String... idxNames) {
        log(" rebuildSecondaryIndex(String ksName, String cfName, String... idxNames)");
    }

    public void resetLocalSchema() throws IOException {
        log(" resetLocalSchema() throws IOException");
        c.post("/storage_service/relocal_schema");
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
    public void setTraceProbability(double probability) {
        log(" setTraceProbability(double probability)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("probability", Double.toString(probability));
        c.post("/storage_service/trace_probability", queryParams);
    }

    /**
     * Returns the configured tracing probability.
     */
    public double getTraceProbability() {
        log(" getTraceProbability()");
        return c.getDoubleValue("/storage_service/trace_probability");
    }

    public void disableAutoCompaction(String ks, String... columnFamilies)
            throws IOException {
        log("disableAutoCompaction(String ks, String... columnFamilies)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        c.delete("/storage_service/auto_compaction/", queryParams);
    }

    public void enableAutoCompaction(String ks, String... columnFamilies)
            throws IOException {
        log("enableAutoCompaction(String ks, String... columnFamilies)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        APIClient.set_query_param(queryParams, "cf",
                APIClient.join(columnFamilies));
        c.post("/storage_service/auto_compaction/", queryParams);

    }

    public void deliverHints(String host) throws UnknownHostException {
        log(" deliverHints(String host) throws UnknownHostException");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("host", host);
        c.post("/storage_service/deliver_hints", queryParams);
    }

    /** Returns the name of the cluster */
    public String getClusterName() {
        log(" getClusterName()");
        return c.getStringValue("/storage_service/cluster_name");
    }

    /** Returns the cluster partitioner */
    public String getPartitionerName() {
        log(" getPartitionerName()");
        return c.getStringValue("/storage_service/partitioner_name");
    }

    /** Returns the threshold for warning of queries with many tombstones */
    public int getTombstoneWarnThreshold() {
        log(" getTombstoneWarnThreshold()");
        return c.getIntValue("/storage_service/tombstone_warn_threshold");
    }

    /** Sets the threshold for warning queries with many tombstones */
    public void setTombstoneWarnThreshold(int tombstoneDebugThreshold) {
        log(" setTombstoneWarnThreshold(int tombstoneDebugThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("debug_threshold",
                Integer.toString(tombstoneDebugThreshold));
        c.post("/storage_service/tombstone_warn_threshold", queryParams);
    }

    /** Returns the threshold for abandoning queries with many tombstones */
    public int getTombstoneFailureThreshold() {
        log(" getTombstoneFailureThreshold()");
        return c.getIntValue("/storage_service/tombstone_failure_threshold");
    }

    /** Sets the threshold for abandoning queries with many tombstones */
    public void setTombstoneFailureThreshold(int tombstoneDebugThreshold) {
        log(" setTombstoneFailureThreshold(int tombstoneDebugThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("debug_threshold",
                Integer.toString(tombstoneDebugThreshold));
        c.post("/storage_service/tombstone_failure_threshold", queryParams);
    }

    /** Returns the threshold for rejecting queries due to a large batch size */
    public int getBatchSizeFailureThreshold() {
        log(" getBatchSizeFailureThreshold()");
        return c.getIntValue("/storage_service/batch_size_failure_threshold");
    }

    /** Sets the threshold for rejecting queries due to a large batch size */
    public void setBatchSizeFailureThreshold(int batchSizeDebugThreshold) {
        log(" setBatchSizeFailureThreshold(int batchSizeDebugThreshold)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("threshold", Integer.toString(batchSizeDebugThreshold));
        c.post("/storage_service/batch_size_failure_threshold", queryParams);
    }

    /**
     * Sets the hinted handoff throttle in kb per second, per delivery thread.
     */
    public void setHintedHandoffThrottleInKB(int throttleInKB) {
        log(" setHintedHandoffThrottleInKB(int throttleInKB)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("throttle", Integer.toString(throttleInKB));
        c.post("/storage_service/hinted_handoff", queryParams);

    }

    @Override
    public void takeMultipleColumnFamilySnapshot(String tag,
            String... columnFamilyList) throws IOException {
        // TODO Auto-generated method stub
        log(" takeMultipleColumnFamilySnapshot");
    }

    @Override
    public int scrub(boolean disableSnapshot, boolean skipCorrupted,
            boolean checkData, String keyspaceName, String... columnFamilies)
                    throws IOException, ExecutionException,
                    InterruptedException {
        // TODO Auto-generated method stub
        log(" scrub()");
        return c.getIntValue("");
    }

    @Override
    public int forceRepairAsync(String keyspace, int parallelismDegree,
            Collection<String> dataCenters, Collection<String> hosts,
            boolean primaryRange, boolean fullRepair,
            String... columnFamilies) {
        // TODO Auto-generated method stub
        log(" forceRepairAsync()");
        Map<String, String> options = new HashMap<String, String>();
        return repairAsync(keyspace, options);
    }

    @Override
    public int forceRepairRangeAsync(String beginToken, String endToken,
            String keyspaceName, int parallelismDegree,
            Collection<String> dataCenters, Collection<String> hosts,
            boolean fullRepair, String... columnFamilies) {
        // TODO Auto-generated method stub
        log(" forceRepairRangeAsync()");
        return c.getIntValue("");
    }

    @Override
    public double getTracingProbability() {
        // TODO Auto-generated method stub
        log(" getTracingProbability()");
        return c.getDoubleValue("");
    }
}
