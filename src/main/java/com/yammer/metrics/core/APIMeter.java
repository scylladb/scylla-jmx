package com.yammer.metrics.core;
/*
 * Copyright (C) 2015 ScyllaDB
 */

import java.util.concurrent.ScheduledExecutorService;

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

/*
 * Modified by ScyllaDB
 */
import java.util.concurrent.TimeUnit;

import javax.json.JsonArray;
import javax.json.JsonObject;

import com.scylladb.jmx.api.APIClient;

public class APIMeter extends Meter {
    public final static long CACHE_DURATION = 1000;

    String url;
    String eventType;
    TimeUnit rateUnit;
    APIClient c = new APIClient();
    long count;
    double oneMinuteRate;
    double fiveMinuteRate;
    double fifteenMinuteRate;
    double meanRate;

    public APIMeter(String url, ScheduledExecutorService tickThread,
            String eventType, TimeUnit rateUnit) {
        super(tickThread, eventType, rateUnit, Clock.defaultClock());
        super.stop();
        this.url = url;
        this.eventType = eventType;
        this.rateUnit = rateUnit;
    }

    public void fromJson(JsonObject obj) {
        JsonArray rates = obj.getJsonArray("rates");
        int i = 0;
        oneMinuteRate = rates.getJsonNumber(i++).doubleValue();
        fiveMinuteRate = rates.getJsonNumber(i++).doubleValue();
        fifteenMinuteRate = rates.getJsonNumber(i++).doubleValue();
        meanRate = obj.getJsonNumber("mean_rate").doubleValue();
        count = obj.getJsonNumber("count").longValue();
    }

    public void update_fields() {
        if (url != null) {
            fromJson(c.getJsonObj(url, null, CACHE_DURATION));
        }
    }

    @Override
    public TimeUnit rateUnit() {
        return rateUnit;
    }

    @Override
    public String eventType() {
        return eventType;
    }

    @Override
    public long count() {
        update_fields();
        return count;
    }

    @Override
    public double fifteenMinuteRate() {
        update_fields();
        return fifteenMinuteRate;
    }

    @Override
    public double fiveMinuteRate() {
        update_fields();
        return fiveMinuteRate;
    }

    @Override
    public double meanRate() {
        update_fields();
        return meanRate;
    }

    @Override
    public double oneMinuteRate() {
        update_fields();
        return oneMinuteRate;
    }

}
