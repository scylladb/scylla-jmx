/*
 * Copyright 2015 Cloudius Systems
 *
 */
package com.yammer.metrics.core;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.json.JsonObject;

import com.scylladb.jmx.api.APIClient;
import com.yammer.metrics.core.Histogram.SampleType;
import com.yammer.metrics.stats.Snapshot;

/**
 * A timer metric which aggregates timing durations and provides duration
 * statistics, plus throughput statistics via {@link Meter}.
 */
public class APITimer extends Timer {
    public final static long CACHE_DURATION = 1000;

    final TimeUnit durationUnit, rateUnit;
    final APIMeter meter;
    final APIHistogram histogram;
    APIClient c = new APIClient();

    private double convertFromNS(double ns) {
        return ns / TimeUnit.NANOSECONDS.convert(1, durationUnit);
    }

    String url;

    public APITimer(String url, ScheduledExecutorService tickThread,
            TimeUnit durationUnit, TimeUnit rateUnit) {
        super(tickThread, durationUnit, rateUnit);
        super.stop();
        this.url = url;
        this.durationUnit = durationUnit;
        this.rateUnit = rateUnit;
        meter = new APIMeter(null, tickThread, "calls", rateUnit);
        histogram = new APIHistogram(null, SampleType.BIASED);
    }

    public void fromJson(JsonObject obj) {
        meter.fromJson(obj.getJsonObject("meter"));
        histogram.updateValue(APIClient.json2histogram(obj.getJsonObject("hist")));
    }

    public void update_fields() {
        if (url != null) {
            fromJson(c.getJsonObj(url, null, CACHE_DURATION));
        }
    }

    @Override
    public double max() {
        update_fields();
        return convertFromNS(histogram.max());
    }

    @Override
    public double min() {
        update_fields();
        return convertFromNS(histogram.min());
    }

    @Override
    public double mean() {
        update_fields();
        return convertFromNS(histogram.mean());
    }

    @Override
    public double stdDev() {
        update_fields();
        return convertFromNS(histogram.stdDev());
    }

    @Override
    public double sum() {
        update_fields();
        return convertFromNS(histogram.sum());
    }

    @Override
    public Snapshot getSnapshot() {
        update_fields();
        return histogram.getSnapshot();
    }

    @Override
    public TimeUnit rateUnit() {
        update_fields();
        return meter.rateUnit();
    }

    @Override
    public String eventType() {
        update_fields();
        return meter.eventType();
    }

    @Override
    public long count() {
        update_fields();
        return meter.count();
    }

    @Override
    public double fifteenMinuteRate() {
        update_fields();
        return meter.fifteenMinuteRate();
    }

    @Override
    public double fiveMinuteRate() {
        update_fields();
        return meter.fiveMinuteRate();
    }

    @Override
    public double meanRate() {
        update_fields();
        return meter.meanRate();
    }

    @Override
    public double oneMinuteRate() {
        update_fields();
        return meter.oneMinuteRate();
    }

}
