package com.scylladb.jmx.utils;
/*
 * Copyright (C) 2015 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 *
 * RecentEstimatedHistogram In the (deprecated) 'recent' functionality, each
 * call to get the values cleans the value.
 *
 * The RecentEstimatedHistogram support recent call to EstimatedHistogram.
 * It holds the latest total values and a call to getBuckets return the delta.
 *
 */
public class RecentEstimatedHistogram extends EstimatedHistogram {
    public RecentEstimatedHistogram() {
    }

    public RecentEstimatedHistogram(int bucketCount) {
        super(bucketCount);
    }

    public RecentEstimatedHistogram(long[] offsets, long[] bucketData) {
        super(offsets, bucketData);
    }

    /**
     * Set the current buckets to new value and return the delta from the last
     * getBuckets call
     *
     * @param bucketData
     *            - new bucket value
     * @return a long[] containing the current histogram difference buckets
     */
    public long[] getBuckets(long[] bucketData) {
        if (bucketData.length == 0) {
            return new long[0];
        }
        final int len = buckets.length();
        long[] rv = new long[len];

        for (int i = 0; i < len; i++) {
            rv[i] = bucketData[i];
            rv[i] -= buckets.getAndSet(i, bucketData[i]);
        }
        return rv;
    }
}
