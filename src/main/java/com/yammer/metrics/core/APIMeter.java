package com.yammer.metrics.core;

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.cloudius.urchin.api.APIClient;

public class APIMeter extends Meter {
    String url;
    private APIClient c = new APIClient();

    public APIMeter(String _url, ScheduledExecutorService tickThread,
            String eventType, TimeUnit rateUnit, Clock clock) {
        super(tickThread, eventType, rateUnit, clock);
        // TODO Auto-generated constructor stub
        url = _url;
    }

    public long get_value() {
        return c.getLongValue(url);
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
    void tick() {
        set(get_value());
        super.tick();
    }

}
