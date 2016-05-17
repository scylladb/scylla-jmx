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
import java.util.concurrent.ExecutionException;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.cassandra.metrics.CacheMetrics;

import com.scylladb.jmx.api.APIClient;

public class CacheService implements CacheServiceMBean {
    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger(CacheService.class.getName());
    private APIClient c = new APIClient();

    public void log(String str) {
        logger.finest(str);
    }

    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=Caches";

    public final CacheMetrics keyCache;
    public final CacheMetrics rowCache;
    public final CacheMetrics counterCache;
    public final static CacheService instance = new CacheService();

    public static CacheService getInstance() {
        return instance;
    }

    private CacheService() {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        try {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        keyCache = new CacheMetrics("KeyCache", null);
        rowCache = new CacheMetrics("RowCache", "row");
        counterCache  = new CacheMetrics("CounterCache", null);
    }

    public int getRowCacheSavePeriodInSeconds() {
        log(" getRowCacheSavePeriodInSeconds()");
        return c.getIntValue("cache_service/row_cache_save_period");
    }

    public void setRowCacheSavePeriodInSeconds(int rcspis) {
        log(" setRowCacheSavePeriodInSeconds(int rcspis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("period", Integer.toString(rcspis));
        c.post("cache_service/row_cache_save_period", queryParams);
    }

    public int getKeyCacheSavePeriodInSeconds() {
        log(" getKeyCacheSavePeriodInSeconds()");
        return c.getIntValue("cache_service/key_cache_save_period");
    }

    public void setKeyCacheSavePeriodInSeconds(int kcspis) {
        log(" setKeyCacheSavePeriodInSeconds(int kcspis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("period", Integer.toString(kcspis));
        c.post("cache_service/key_cache_save_period", queryParams);
    }

    public int getCounterCacheSavePeriodInSeconds() {
        log(" getCounterCacheSavePeriodInSeconds()");
        return c.getIntValue("cache_service/counter_cache_save_period");
    }

    public void setCounterCacheSavePeriodInSeconds(int ccspis) {
        log(" setCounterCacheSavePeriodInSeconds(int ccspis)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("ccspis", Integer.toString(ccspis));
        c.post("cache_service/counter_cache_save_period", queryParams);
    }

    public int getRowCacheKeysToSave() {
        log(" getRowCacheKeysToSave()");
        return c.getIntValue("cache_service/row_cache_keys_to_save");
    }

    public void setRowCacheKeysToSave(int rckts) {
        log(" setRowCacheKeysToSave(int rckts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("rckts", Integer.toString(rckts));
        c.post("cache_service/row_cache_keys_to_save", queryParams);
    }

    public int getKeyCacheKeysToSave() {
        log(" getKeyCacheKeysToSave()");
        return c.getIntValue("cache_service/key_cache_keys_to_save");
    }

    public void setKeyCacheKeysToSave(int kckts) {
        log(" setKeyCacheKeysToSave(int kckts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("kckts", Integer.toString(kckts));
        c.post("cache_service/key_cache_keys_to_save", queryParams);
    }

    public int getCounterCacheKeysToSave() {
        log(" getCounterCacheKeysToSave()");
        return c.getIntValue("cache_service/counter_cache_keys_to_save");
    }

    public void setCounterCacheKeysToSave(int cckts) {
        log(" setCounterCacheKeysToSave(int cckts)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("cckts", Integer.toString(cckts));
        c.post("cache_service/counter_cache_keys_to_save", queryParams);
    }

    /**
     * invalidate the key cache; for use after invalidating row cache
     */
    public void invalidateKeyCache() {
        log(" invalidateKeyCache()");
        c.post("cache_service/invalidate_key_cache");
    }

    /**
     * invalidate the row cache; for use after bulk loading via BinaryMemtable
     */
    public void invalidateRowCache() {
        log(" invalidateRowCache()");
        c.post("cache_service/invalidate_row_cache");
    }

    public void invalidateCounterCache() {
        log(" invalidateCounterCache()");
        c.post("cache_service/invalidate_counter_cache");
    }

    public void setRowCacheCapacityInMB(long capacity) {
        log(" setRowCacheCapacityInMB(long capacity)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Long.toString(capacity));
        c.post("cache_service/row_cache_capacity", queryParams);
    }

    public void setKeyCacheCapacityInMB(long capacity) {
        log(" setKeyCacheCapacityInMB(long capacity)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Long.toString(capacity));
        c.post("cache_service/key_cache_capacity", queryParams);
    }

    public void setCounterCacheCapacityInMB(long capacity) {
        log(" setCounterCacheCapacityInMB(long capacity)");
        MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<String, String>();
        queryParams.add("capacity", Long.toString(capacity));
        c.post("cache_service/counter_cache_capacity_in_mb", queryParams);
    }

    /**
     * save row and key caches
     *
     * @throws ExecutionException
     *             when attempting to retrieve the result of a task that aborted
     *             by throwing an exception
     * @throws InterruptedException
     *             when a thread is waiting, sleeping, or otherwise occupied,
     *             and the thread is interrupted, either before or during the
     *             activity.
     */
    public void saveCaches() throws ExecutionException, InterruptedException {
        log(" saveCaches() throws ExecutionException, InterruptedException");
        c.post("cache_service/save_caches");
    }

    //
    // remaining methods are provided for backwards compatibility; modern
    // clients should use CacheMetrics instead
    //

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hits
     */
    @Deprecated
    public long getKeyCacheHits() {
        log(" getKeyCacheHits()");
        return keyCache.hits.count();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hits
     */
    @Deprecated
    public long getRowCacheHits() {
        log(" getRowCacheHits()");
        return rowCache.hits.count();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#requests
     */
    @Deprecated
    public long getKeyCacheRequests() {
        log(" getKeyCacheRequests()");
        return keyCache.requests.count();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#requests
     */
    @Deprecated
    public long getRowCacheRequests() {
        log(" getRowCacheRequests()");
        return rowCache.requests.count();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hitRate
     */
    @Deprecated
    public double getKeyCacheRecentHitRate() {
        log(" getKeyCacheRecentHitRate()");
        return keyCache.getRecentHitRate();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#hitRate
     */
    @Deprecated
    public double getRowCacheRecentHitRate() {
        log(" getRowCacheRecentHitRate()");
        return rowCache.getRecentHitRate();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getRowCacheCapacityInMB() {
        log(" getRowCacheCapacityInMB()");
        return getRowCacheCapacityInBytes() / 1024 / 1024;
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getRowCacheCapacityInBytes() {
        log(" getRowCacheCapacityInBytes()");
        return rowCache.capacity.value();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getKeyCacheCapacityInMB() {
        log(" getKeyCacheCapacityInMB()");
        return getKeyCacheCapacityInBytes() / 1024 / 1024;
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getKeyCacheCapacityInBytes() {
        log(" getKeyCacheCapacityInBytes()");
        return keyCache.capacity.value();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#size
     */
    @Deprecated
    public long getRowCacheSize() {
        log(" getRowCacheSize()");
        return rowCache.size.value();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#entries
     */
    @Deprecated
    public long getRowCacheEntries() {
        log(" getRowCacheEntries()");
        return rowCache.size.value();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#size
     */
    @Deprecated
    public long getKeyCacheSize() {
        log(" getKeyCacheSize()");
        return keyCache.size.value();
    }

    /**
     * @see org.apache.cassandra.metrics.CacheMetrics#entries
     */
    @Deprecated
    public long getKeyCacheEntries() {
        log(" getKeyCacheEntries()");
        return keyCache.size.value();
    }
}
