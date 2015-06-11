package com.yammer.metrics.core;

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

import com.cloudius.urchin.api.APIClient;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;

public class APIHistogram extends Histogram {
    long last_update = 0;
    static final long UPDATE_INTERVAL = 50;
    String url;
    private APIClient c = new APIClient();

    public APIHistogram(String _url, Sample sample) {
        super(sample);
        url = _url;
    }

    public APIHistogram(String _url, SampleType type) {
        super(type);
        url = _url;
    }

    public void update() {
        long now = System.currentTimeMillis();
        if (now - last_update < UPDATE_INTERVAL) {
            return;
        }
        last_update = now;
        clear();
        long[] vals = c.getLongArrValue(url);
        for (long v : vals) {
            update(v);
        }
    }

    /**
     * Returns the number of values recorded.
     *
     * @return the number of values recorded
     */
    public long count() {
        update();
        return super.count();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yammer.metrics.core.Summarizable#max()
     */
    @Override
    public double max() {
        update();
        return super.max();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yammer.metrics.core.Summarizable#min()
     */
    @Override
    public double min() {
        update();
        return super.min();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yammer.metrics.core.Summarizable#mean()
     */
    @Override
    public double mean() {
        update();
        return super.mean();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yammer.metrics.core.Summarizable#stdDev()
     */
    @Override
    public double stdDev() {
        update();
        return super.stdDev();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.yammer.metrics.core.Summarizable#sum()
     */
    @Override
    public double sum() {
        update();
        return super.sum();
    }

    @Override
    public Snapshot getSnapshot() {
        update();
        return super.getSnapshot();
    }
}
