package com.yammer.metrics.core;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*
 * Copyright 2015 ScyllaDB
 *
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

public class APISettableMeter extends Meter {

    public APISettableMeter(ScheduledExecutorService tickThread,
            String eventType, TimeUnit rateUnit, Clock clock) {
        super(tickThread, eventType, rateUnit, clock);
    }

    // Meter doesn't have a set value method.
    // to mimic it, we clear the old value and set it to a new one.
    // This is safe because the only this method would be used
    // to update the values
    public long set(long new_value) {
        long res = super.count();
        mark(-res);
        mark(new_value);
        return res;
    }

    @Override
    public void tick() {
        super.tick();
    }
}
