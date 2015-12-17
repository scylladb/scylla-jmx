package org.apache.cassandra.metrics;
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

import javax.ws.rs.core.MultivaluedMap;

import com.scylladb.jmx.api.APIClient;
import com.scylladb.jmx.utils.EstimatedHistogram;

public class EstimatedHistogramWrapper {
    private APIClient c = new APIClient();
    private String url;
    private MultivaluedMap<String, String> queryParams;
    private static final int DURATION = 50;
    private int duration;
    public EstimatedHistogramWrapper(String url, MultivaluedMap<String, String> queryParams, int duration) {
        this.url = url;
        this.queryParams = queryParams;
        this.duration = duration;

    }
    public EstimatedHistogramWrapper(String url) {
        this(url, null, DURATION);

    }
    public EstimatedHistogramWrapper(String url, int duration) {
        this(url, null, duration);

    }
    public EstimatedHistogram get() {
        return c.getEstimatedHistogram(url, queryParams, duration);
    }

    public long[] getBuckets(boolean reset) {
        return get().getBuckets(reset);
    }
}
