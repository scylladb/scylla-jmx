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

    @Override
    public long count() {
        return c.getLongValue(url);
    }

}
