/*
 * Copyright 2015 Cloudius Systems
 *
 */
package com.yammer.metrics.core;

import java.lang.reflect.Field;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.core.Histogram.SampleType;

/**
 * A timer metric which aggregates timing durations and provides duration
 * statistics, plus throughput statistics via {@link Meter}.
 */
public class APITimer extends Timer {

    public APITimer(String url, ScheduledExecutorService tickThread,
            TimeUnit durationUnit, TimeUnit rateUnit) {
        super(tickThread, durationUnit, rateUnit);
        setHistogram(url);
    }

    public APITimer(String url, ScheduledExecutorService tickThread,
            TimeUnit durationUnit, TimeUnit rateUnit, Clock clock) {
        super(tickThread, durationUnit, rateUnit, clock);
        setHistogram(url);
    }

    private void setHistogram(String url) {
        Field histogram;
        try {
            histogram = Timer.class.getDeclaredField("histogram");
            histogram.setAccessible(true);
            histogram.set(this, new APIHistogram(url, SampleType.BIASED));
        } catch (NoSuchFieldException | SecurityException
                | IllegalArgumentException | IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
