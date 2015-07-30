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

import java.lang.management.ManagementFactory;
import java.util.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.core.MultivaluedMap;

import com.cloudius.urchin.api.APIClient;
import com.sun.jersey.core.util.MultivaluedMapImpl;

import org.apache.cassandra.metrics.*;

public class StorageProxy implements StorageProxyMBean {
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(StorageProxy.class.getName());

    private APIClient c = new APIClient();

    public void log(String str) {
        logger.info(str);
    }

    private static final StorageProxy instance = new StorageProxy();

    public static StorageProxy getInstance() {
        return instance;
    }

    public static final String UNREACHABLE = "UNREACHABLE";

    private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics(
            "storage_proxy/metrics/read/", "Read");
    private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics(
            "storage_proxy/metrics/range/", "RangeSlice");
    private static final ClientRequestMetrics writeMetrics = new ClientRequestMetrics(
            "storage_proxy/metrics/write/", "Write");
    private static final CASClientRequestMetrics casWriteMetrics = new CASClientRequestMetrics(
            "storage_proxy/metrics/cas_write/", "CASWrite");
    private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics(
            "storage_proxy/metrics/cas_read/", "CASRead");

    private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.10;

    private StorageProxy() {
    }

    static {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(instance, new ObjectName(MBEAN_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#lastOpCount
     */
    @Deprecated
    public long getReadOperations() {
        log(" getReadOperations()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#totalLatencyHistogram
     */
    @Deprecated
    public long getTotalReadLatencyMicros() {
        log(" getTotalReadLatencyMicros()");
        return c.getLongValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#recentLatencyHistogram
     */
    @Deprecated
    public double getRecentReadLatencyMicros() {
        log(" getRecentReadLatencyMicros()");
        return c.getDoubleValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#totalLatencyHistogram
     */
    @Deprecated
    public long[] getTotalReadLatencyHistogramMicros() {
        log(" getTotalReadLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    /**
     * @see org.apache.cassandra.metrics.LatencyMetrics#recentLatencyHistogram
     */
    @Deprecated
    public long[] getRecentReadLatencyHistogramMicros() {
        log(" getRecentReadLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    @Deprecated
    public long getRangeOperations() {
        log(" getRangeOperations()");
        return c.getLongValue("");
    }

    @Deprecated
    public long getTotalRangeLatencyMicros() {
        log(" getTotalRangeLatencyMicros()");
        return c.getLongValue("");
    }

    @Deprecated
    public double getRecentRangeLatencyMicros() {
        log(" getRecentRangeLatencyMicros()");
        return c.getDoubleValue("");
    }

    @Deprecated
    public long[] getTotalRangeLatencyHistogramMicros() {
        log(" getTotalRangeLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    @Deprecated
    public long[] getRecentRangeLatencyHistogramMicros() {
        log(" getRecentRangeLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    @Deprecated
    public long getWriteOperations() {
        log(" getWriteOperations()");
        return c.getLongValue("");
    }

    @Deprecated
    public long getTotalWriteLatencyMicros() {
        log(" getTotalWriteLatencyMicros()");
        return c.getLongValue("");
    }

    @Deprecated
    public double getRecentWriteLatencyMicros() {
        log(" getRecentWriteLatencyMicros()");
        return c.getDoubleValue("");
    }

    @Deprecated
    public long[] getTotalWriteLatencyHistogramMicros() {
        log(" getTotalWriteLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    @Deprecated
    public long[] getRecentWriteLatencyHistogramMicros() {
        log(" getRecentWriteLatencyHistogramMicros()");
        return c.getLongArrValue("");
    }

    public long getTotalHints() {
        log(" getTotalHints()");
        return c.getLongValue("storage_proxy/total_hints");
    }

    public boolean getHintedHandoffEnabled() {
        log(" getHintedHandoffEnabled()");
        return c.getBooleanValue("storage_proxy/hinted_handoff_enabled");
    }

    public Set<String> getHintedHandoffEnabledByDC() {
        log(" getHintedHandoffEnabledByDC()");
        return c.getSetStringValue(
                "storage_proxy/hinted_handoff_enabled_by_dc");
    }

    public void setHintedHandoffEnabled(boolean b) {
        log(" setHintedHandoffEnabled(boolean b)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("enable", Boolean.toString(b));
        c.post("storage_proxy/hinted_handoff_enabled", queryParams);
    }

    public void setHintedHandoffEnabledByDCList(String dcs) {
        log(" setHintedHandoffEnabledByDCList(String dcs)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("dcs", dcs);
        c.post("storage_proxy/hinted_handoff_enabled_by_dc_list");
    }

    public int getMaxHintWindow() {
        log(" getMaxHintWindow()");
        return c.getIntValue("storage_proxy/max_hint_window");
    }

    public void setMaxHintWindow(int ms) {
        log(" setMaxHintWindow(int ms)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("ms", Integer.toString(ms));
        c.post("storage_proxy/max_hint_window", queryParams);
    }

    public int getMaxHintsInProgress() {
        log(" getMaxHintsInProgress()");
        return c.getIntValue("storage_proxy/max_hints_in_progress");
    }

    public void setMaxHintsInProgress(int qs) {
        log(" setMaxHintsInProgress(int qs)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("qs", Integer.toString(qs));
        c.post("storage_proxy/max_hints_in_progress", queryParams);
    }

    public int getHintsInProgress() {
        log(" getHintsInProgress()");
        return c.getIntValue("storage_proxy/hints_in_progress");
    }

    public Long getRpcTimeout() {
        log(" getRpcTimeout()");
        return c.getLongValue("storage_proxy/rpc_timeout");
    }

    public void setRpcTimeout(Long timeoutInMillis) {
        log(" setRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/rpc_timeout", queryParams);
    }

    public Long getReadRpcTimeout() {
        log(" getReadRpcTimeout()");
        return c.getLongValue("storage_proxy/read_rpc_timeout");
    }

    public void setReadRpcTimeout(Long timeoutInMillis) {
        log(" setReadRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/read_rpc_timeout", queryParams);
    }

    public Long getWriteRpcTimeout() {
        log(" getWriteRpcTimeout()");
        return c.getLongValue("storage_proxy/write_rpc_timeout");
    }

    public void setWriteRpcTimeout(Long timeoutInMillis) {
        log(" setWriteRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/write_rpc_timeout", queryParams);
    }

    public Long getCounterWriteRpcTimeout() {
        log(" getCounterWriteRpcTimeout()");
        return c.getLongValue("storage_proxy/counter_write_rpc_timeout");
    }

    public void setCounterWriteRpcTimeout(Long timeoutInMillis) {
        log(" setCounterWriteRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/counter_write_rpc_timeout", queryParams);
    }

    public Long getCasContentionTimeout() {
        log(" getCasContentionTimeout()");
        return c.getLongValue("storage_proxy/cas_contention_timeout");
    }

    public void setCasContentionTimeout(Long timeoutInMillis) {
        log(" setCasContentionTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/cas_contention_timeout", queryParams);
    }

    public Long getRangeRpcTimeout() {
        log(" getRangeRpcTimeout()");
        return c.getLongValue("storage_proxy/range_rpc_timeout");
    }

    public void setRangeRpcTimeout(Long timeoutInMillis) {
        log(" setRangeRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/range_rpc_timeout", queryParams);
    }

    public Long getTruncateRpcTimeout() {
        log(" getTruncateRpcTimeout()");
        return c.getLongValue("storage_proxy/truncate_rpc_timeout");
    }

    public void setTruncateRpcTimeout(Long timeoutInMillis) {
        log(" setTruncateRpcTimeout(Long timeoutInMillis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedMapImpl();
        queryParams.add("timeout", Long.toString(timeoutInMillis));
        c.post("storage_proxy/truncate_rpc_timeout", queryParams);
    }

    public void reloadTriggerClasses() {
        log(" reloadTriggerClasses()");
        c.post("storage_proxy/reload_trigger_classes");
    }

    public long getReadRepairAttempted() {
        log(" getReadRepairAttempted()");
        return c.getLongValue("storage_proxy/read_repair_attempted");
    }

    public long getReadRepairRepairedBlocking() {
        log(" getReadRepairRepairedBlocking()");
        return c.getLongValue("storage_proxy/read_repair_repaired_blocking");
    }

    public long getReadRepairRepairedBackground() {
        log(" getReadRepairRepairedBackground()");
        return c.getLongValue("storage_proxy/read_repair_repaired_background");
    }

    /** Returns each live node's schema version */
    public Map<String, List<String>> getSchemaVersions() {
        log(" getSchemaVersions()");
        return c.getMapStringListStrValue("storage_proxy/schema_versions");
    }

    @Override
    public void setNativeTransportMaxConcurrentConnections(
            Long nativeTransportMaxConcurrentConnections) {
        // TODO Auto-generated method stub
        log(" setNativeTransportMaxConcurrentConnections()");

    }

    @Override
    public Long getNativeTransportMaxConcurrentConnections() {
        // TODO Auto-generated method stub
        log(" getNativeTransportMaxConcurrentConnections()");
        return c.getLongValue("");
    }

}
