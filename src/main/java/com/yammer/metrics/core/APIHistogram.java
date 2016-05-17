package com.yammer.metrics.core;

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
 */

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.json.JsonObject;

import com.scylladb.jmx.api.APIClient;
import com.yammer.metrics.stats.Sample;
import com.yammer.metrics.stats.Snapshot;

public class APIHistogram extends Histogram {
    Field countField;
    Field minField;
    Field maxField;
    Field sumField;
    Field varianceField;
    Field sampleField;

    long last_update = 0;
    static final long UPDATE_INTERVAL = 50;
    long updateInterval;
    String url;
    private APIClient c = new APIClient();

    private void setFields() {
        try {
            minField = Histogram.class.getDeclaredField("min");
            minField.setAccessible(true);
            maxField = Histogram.class.getDeclaredField("max");
            maxField.setAccessible(true);
            sumField = Histogram.class.getDeclaredField("sum");
            sumField.setAccessible(true);
            varianceField = Histogram.class.getDeclaredField("variance");
            varianceField.setAccessible(true);
            sampleField = Histogram.class.getDeclaredField("sample");
            sampleField.setAccessible(true);
            countField = Histogram.class.getDeclaredField("count");
            countField.setAccessible(true);
            try {
                getCount().set(0);
            } catch (IllegalArgumentException | IllegalAccessException e) {
                // There's no reason to get here
                // and there's nothing we can do even if we would
            }
        } catch (NoSuchFieldException | SecurityException e) {
            e.printStackTrace();
        }
    }

    public AtomicLong getMin() throws IllegalArgumentException,
            IllegalAccessException {
        return (AtomicLong) minField.get(this);
    }

    public AtomicLong getMax() throws IllegalArgumentException,
            IllegalAccessException {
        return (AtomicLong) maxField.get(this);
    }

    public AtomicLong getSum() throws IllegalArgumentException,
            IllegalAccessException {
        return (AtomicLong) sumField.get(this);
    }

    public AtomicLong getCount() throws IllegalArgumentException,
            IllegalAccessException {
        return (AtomicLong) countField.get(this);
    }

    @SuppressWarnings("unchecked")
    public AtomicReference<double[]> getVariance()
            throws IllegalArgumentException, IllegalAccessException {
        return (AtomicReference<double[]>) varianceField.get(this);
    }

    public Sample getSample() throws IllegalArgumentException,
            IllegalAccessException {
        return (Sample) sampleField.get(this);
    }

    public APIHistogram(String url, Sample sample) {
        super(sample);
        setFields();
        this.url = url;
    }

    public APIHistogram(String url, SampleType type, long updateInterval) {
        super(type);
        setFields();
        this.url = url;
        this.updateInterval = updateInterval;
    }

    public APIHistogram(String url, SampleType type) {
        this(url, type, UPDATE_INTERVAL);
    }

    public void updateValue(HistogramValues vals) {
        try {
            if (vals.sample != null) {
                for (long v : vals.sample) {
                    getSample().update(v);
                }
            }
            getCount().set(vals.count);
            getMax().set(vals.max);
            getMin().set(vals.min);
            getSum().set(vals.sum);
            double[] newValue = new double[2];
            newValue[0] = vals.mean;
            newValue[1] = vals.variance;
            getVariance().getAndSet(newValue);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public void update() {
        if (url == null) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - last_update < UPDATE_INTERVAL) {
            return;
        }
        last_update = now;
        clear();
        JsonObject obj = c.getJsonObj(url, null);
        if (obj.containsKey("hist")) {
            updateValue(APIClient.json2histogram(obj.getJsonObject("hist")));
        } else {
            updateValue(APIClient.json2histogram(obj));
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
