package com.yammer.metrics.core;
/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

import com.cloudius.urchin.api.APIClient;
import com.yammer.metrics.core.Counter;

public class APICounter extends Counter {
    String url;
    private APIClient c = new APIClient();
    
    public APICounter(String _url) {
        super();
        url = _url;
    }
    /**
     * Returns the counter's current value.
     *
     * @return the counter's current value
     */
    public long count() {
        return c.getLongValue(url);
    }


}
