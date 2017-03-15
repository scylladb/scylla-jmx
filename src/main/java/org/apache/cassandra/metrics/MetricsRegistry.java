/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.metrics;

import static com.scylladb.jmx.api.APIClient.getReader;
import static java.lang.Math.floor;
import static java.util.logging.Level.SEVERE;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import com.scylladb.jmx.api.APIClient;

/**
 * Makes integrating 3.0 metrics API with 2.0.
 * <p>
 * The 3.0 API comes with poor JMX integration
 * </p>
 */
public class MetricsRegistry {
    private static final long CACHE_DURATION = 1000;
    private static final long UPDATE_INTERVAL = 50;

    private static final Logger logger = Logger.getLogger(MetricsRegistry.class.getName());

    private final APIClient client;
    private final MBeanServer mBeanServer;

    public MetricsRegistry(APIClient client, MBeanServer mBeanServer) {
        this.client = client;
        this.mBeanServer = mBeanServer;
    }

    public MetricsRegistry(MetricsRegistry other) {
        this(other.client, other.mBeanServer);
    }

    public MetricMBean gauge(String url) {
        return gauge(Long.class, url);
    }

    public <T> MetricMBean gauge(Class<T> type, final String url) {
        return gauge(getReader(type), url);
    }

    public <T> MetricMBean gauge(final BiFunction<APIClient, String, T> function, final String url) {
        return gauge(c -> function.apply(c, url));
    }

    public <T> MetricMBean gauge(final Function<APIClient, T> function) {
        return gauge(() -> function.apply(client));
    }

    private class JmxGauge implements JmxGaugeMBean {
        private final Supplier<?> function;

        public JmxGauge(Supplier<?> function) {
            this.function = function;
        }

        @Override
        public Object getValue() {
            return function.get();
        }
    }

    public <T> MetricMBean gauge(final Supplier<T> function) {
        return new JmxGauge(function);
    }

    /**
     * Default approach to register is to actually register/add to
     * {@link MBeanServer} For unbind phase, override here.
     * 
     * @param bean
     * @param objectNames
     */
    public void register(Supplier<MetricMBean> f, ObjectName... objectNames) {
        MetricMBean bean = f.get();
        for (ObjectName name : objectNames) {
            try {
                mBeanServer.registerMBean(bean, name);
            } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
                logger.log(SEVERE, "Could not register mbean", e);
            }
        }
    }

    private class JmxCounter implements JmxCounterMBean {
        private final String url;

        public JmxCounter(String url) {
            super();
            this.url = url;
        }

        @Override
        public long getCount() {
            return client.getLongValue(url);
        }
    }

    public MetricMBean counter(final String url) {
        return new JmxCounter(url);
    }

    private abstract class IntermediatelyUpdated {
        private final long interval;
        private final Supplier<JsonObject> supplier;
        private long lastUpdate;

        public IntermediatelyUpdated(String url, long interval) {
            this.supplier = () -> client.getJsonObj(url, null);
            this.interval = interval;
        }

        public IntermediatelyUpdated(Supplier<JsonObject> supplier, long interval) {
            this.supplier = supplier;
            this.interval = interval;
        }

        public abstract void update(JsonObject obj);

        public final void update() {
            long now = System.currentTimeMillis();
            if (now - lastUpdate < interval) {
                return;
            }
            try {
                JsonObject obj = supplier.get();
                update(obj);
            } finally {
                lastUpdate = now;
            }
        }
    }

    private static class Meter {
        public final long count;
        public final double oneMinuteRate;
        public final double fiveMinuteRate;
        public final double fifteenMinuteRate;
        public final double meanRate;

        public Meter(long count, double oneMinuteRate, double fiveMinuteRate, double fifteenMinuteRate,
                double meanRate) {
            this.count = count;
            this.oneMinuteRate = oneMinuteRate;
            this.fiveMinuteRate = fiveMinuteRate;
            this.fifteenMinuteRate = fifteenMinuteRate;
            this.meanRate = meanRate;
        }

        public Meter() {
            this(0, 0, 0, 0, 0);
        }

        public Meter(JsonObject obj) {
            JsonArray rates = obj.getJsonArray("rates");
            oneMinuteRate = rates.getJsonNumber(0).doubleValue();
            fiveMinuteRate = rates.getJsonNumber(1).doubleValue();
            fifteenMinuteRate = rates.getJsonNumber(2).doubleValue();
            meanRate = obj.getJsonNumber("mean_rate").doubleValue();
            count = obj.getJsonNumber("count").longValue();
        }
    }

    private static final TimeUnit RATE_UNIT = TimeUnit.SECONDS;
    private static final TimeUnit DURATION_UNIT = TimeUnit.MICROSECONDS;
    private static final TimeUnit API_DURATION_UNIT = TimeUnit.MICROSECONDS;
    private static final double DURATION_FACTOR = 1.0 / API_DURATION_UNIT.convert(1, DURATION_UNIT);

    private static double toDuration(double micro) {
        return micro * DURATION_FACTOR;
    }

    private static String unitString(TimeUnit u) {
        String s = u.toString().toLowerCase(Locale.US);
        return s.substring(0, s.length() - 1);
    }

    private class JmxMeter extends IntermediatelyUpdated implements JmxMeterMBean {
        private Meter meter = new Meter();

        public JmxMeter(String url, long interval) {
            super(url, interval);
        }

        public JmxMeter(Supplier<JsonObject> supplier, long interval) {
            super(supplier, interval);
        }

        @Override
        public void update(JsonObject obj) {
            meter = new Meter(obj);
        }

        @Override
        public long getCount() {
            update();
            return meter.count;
        }

        @Override
        public double getMeanRate() {
            update();
            return meter.meanRate;
        }

        @Override
        public double getOneMinuteRate() {
            update();
            return meter.oneMinuteRate;
        }

        @Override
        public double getFiveMinuteRate() {
            update();
            return meter.fiveMinuteRate;
        }

        @Override
        public double getFifteenMinuteRate() {
            update();
            return meter.fifteenMinuteRate;
        }

        @Override
        public String getRateUnit() {
            return "event/" + unitString(RATE_UNIT);
        }
    }

    public MetricMBean meter(String url) {
        return new JmxMeter(url, CACHE_DURATION);
    }

    private static long[] asLongArray(JsonArray a) {
        return a.getValuesAs(JsonNumber.class).stream().mapToLong(n -> n.longValue()).toArray();
    }

    private static interface Samples {
        default double getValue(double quantile) {
            return 0;
        }

        default long[] getValues() {
            return new long[0];
        }
    }

    private static class BufferSamples implements Samples {
        private final long[] samples;

        public BufferSamples(long[] samples) {
            this.samples = samples;
            Arrays.sort(this.samples);
        }

        @Override
        public long[] getValues() {
            return samples;
        }

        @Override
        public double getValue(double quantile) {
            if (quantile < 0.0 || quantile > 1.0) {
                throw new IllegalArgumentException(quantile + " is not in [0..1]");
            }

            if (samples.length == 0) {
                return 0.0;
            }

            final double pos = quantile * (samples.length + 1);

            if (pos < 1) {
                return samples[0];
            }

            if (pos >= samples.length) {
                return samples[samples.length - 1];
            }

            final double lower = samples[(int) pos - 1];
            final double upper = samples[(int) pos];
            return lower + (pos - floor(pos)) * (upper - lower);
        }
    }

    private static class Histogram {
        private final long count;
        private final long min;
        private final long max;
        private final double mean;
        private final double stdDev;

        private final Samples samples;

        public Histogram(long count, long min, long max, double mean, double stdDev, Samples samples) {
            this.count = count;
            this.min = min;
            this.max = max;
            this.mean = mean;
            this.stdDev = stdDev;
            this.samples = samples;
        }

        public Histogram() {
            this(0, 0, 0, 0, 0, new Samples() {
            });
        }

        public Histogram(JsonObject obj) {
            this(obj.getJsonNumber("count").longValue(), obj.getJsonNumber("min").longValue(),
                    obj.getJsonNumber("max").longValue(), obj.getJsonNumber("mean").doubleValue(),
                    obj.getJsonNumber("variance").doubleValue(), new BufferSamples(getValues(obj)));
        }

        public Histogram(EstimatedHistogram h) {
            this(h.count(), h.min(), h.max(), h.mean(), 0, h);
        }

        private static long[] getValues(JsonObject obj) {
            JsonArray arr = obj.getJsonArray("sample");
            if (arr != null) {
                return asLongArray(arr);
            }
            return new long[0];
        }

        public long[] getValues() {
            return samples.getValues();
        }

        // Origin (and previous iterations of scylla-jxm)
        // uses biased/ExponentiallyDecaying measurements
        // for the history & quantile resolution.
        // However, for use that is just gobbletigook, since
        // we, at occasions of being asked, and when certain time
        // has passed, ask the actual scylla server for a
        // "values" buffer. A buffer with no information whatsoever
        // on how said values correlate to actual sampling
        // time.
        // So, applying time weights at this level is just
        // wrong. We can just as well treat this as a uniform
        // distribution.
        // Obvious improvement: Send time/value tuples instead.
        public double getValue(double quantile) {
            return samples.getValue(quantile);
        }

        public long getCount() {
            return count;
        }

        public long getMin() {
            return min;
        }

        public long getMax() {
            return max;
        }

        public double getMean() {
            return mean;
        }

        public double getStdDev() {
            return stdDev;
        }
    }

    private static class EstimatedHistogram implements Samples {
        /**
         * The series of values to which the counts in `buckets` correspond: 1,
         * 2, 3, 4, 5, 6, 7, 8, 10, 12, 14, 17, 20, etc. Thus, a `buckets` of
         * [0, 0, 1, 10] would mean we had seen one value of 3 and 10 values of
         * 4.
         *
         * The series starts at 1 and grows by 1.2 each time (rounding and
         * removing duplicates). It goes from 1 to around 36M by default
         * (creating 90+1 buckets), which will give us timing resolution from
         * microseconds to 36 seconds, with less precision as the numbers get
         * larger.
         *
         * Each bucket represents values from (previous bucket offset, current
         * offset].
         */
        private final long[] bucketOffsets;
        // buckets is one element longer than bucketOffsets -- the last element
        // is
        // values greater than the last offset
        private long[] buckets;

        public EstimatedHistogram(JsonObject obj) {
            this(asLongArray(obj.getJsonArray("bucket_offsets")), asLongArray(obj.getJsonArray("buckets")));
        }

        public EstimatedHistogram(long[] offsets, long[] bucketData) {
            assert bucketData.length == offsets.length + 1;
            bucketOffsets = offsets;
            buckets = bucketData;
        }

        /**
         * @return the smallest value that could have been added to this
         *         histogram
         */
        public long min() {
            for (int i = 0; i < buckets.length; i++) {
                if (buckets[i] > 0) {
                    return i == 0 ? 0 : 1 + bucketOffsets[i - 1];
                }
            }
            return 0;
        }

        /**
         * @return the largest value that could have been added to this
         *         histogram. If the histogram overflowed, returns
         *         Long.MAX_VALUE.
         */
        public long max() {
            int lastBucket = buckets.length - 1;
            if (buckets[lastBucket] > 0) {
                return Long.MAX_VALUE;
            }

            for (int i = lastBucket - 1; i >= 0; i--) {
                if (buckets[i] > 0) {
                    return bucketOffsets[i];
                }
            }
            return 0;
        }

        @Override
        public long[] getValues() {
            return buckets;
        }

        /**
         * @param percentile
         * @return estimated value at given percentile
         */
        @Override
        public double getValue(double percentile) {
            assert percentile >= 0 && percentile <= 1.0;
            int lastBucket = buckets.length - 1;
            if (buckets[lastBucket] > 0) {
                throw new IllegalStateException("Unable to compute when histogram overflowed");
            }

            long pcount = (long) Math.floor(count() * percentile);
            if (pcount == 0) {
                return 0;
            }

            long elements = 0;
            for (int i = 0; i < lastBucket; i++) {
                elements += buckets[i];
                if (elements >= pcount) {
                    return bucketOffsets[i];
                }
            }
            return 0;
        }

        /**
         * @return the mean histogram value (average of bucket offsets, weighted
         *         by count)
         * @throws IllegalStateException
         *             if any values were greater than the largest bucket
         *             threshold
         */
        public long mean() {
            int lastBucket = buckets.length - 1;
            if (buckets[lastBucket] > 0) {
                throw new IllegalStateException("Unable to compute ceiling for max when histogram overflowed");
            }

            long elements = 0;
            long sum = 0;
            for (int i = 0; i < lastBucket; i++) {
                long bCount = buckets[i];
                elements += bCount;
                sum += bCount * bucketOffsets[i];
            }

            return (long) Math.ceil((double) sum / elements);
        }

        /**
         * @return the total number of non-zero values
         */
        public long count() {
            return Arrays.stream(buckets).sum();
        }

        /**
         * @return true if this histogram has overflowed -- that is, a value
         *         larger than our largest bucket could bound was added
         */
        @SuppressWarnings("unused")
        public boolean isOverflowed() {
            return buckets[buckets.length - 1] > 0;
        }

    }

    private class JmxHistogram extends IntermediatelyUpdated implements JmxHistogramMBean {
        private Histogram histogram = new Histogram();

        public JmxHistogram(String url, long interval) {
            super(url, interval);
        }

        @Override
        public void update(JsonObject obj) {
            if (obj.containsKey("hist")) {
                obj = obj.getJsonObject("hist");
            }
            if (obj.containsKey("buckets")) {
                histogram = new Histogram(new EstimatedHistogram(obj));
            } else {
                histogram = new Histogram(obj);
            }
        }

        @Override
        public long getCount() {
            update();
            return histogram.getCount();
        }

        @Override
        public long getMin() {
            update();
            return histogram.getMin();
        }

        @Override
        public long getMax() {
            update();
            return histogram.getMax();
        }

        @Override
        public double getMean() {
            update();
            return histogram.getMean();
        }

        @Override
        public double getStdDev() {
            update();
            return histogram.getStdDev();
        }

        @Override
        public double get50thPercentile() {
            update();
            return histogram.getValue(.5);
        }

        @Override
        public double get75thPercentile() {
            update();
            return histogram.getValue(.75);
        }

        @Override
        public double get95thPercentile() {
            update();
            return histogram.getValue(.95);
        }

        @Override
        public double get98thPercentile() {
            update();
            return histogram.getValue(.98);
        }

        @Override
        public double get99thPercentile() {
            update();
            return histogram.getValue(.99);
        }

        @Override
        public double get999thPercentile() {
            update();
            return histogram.getValue(.999);
        }

        @Override
        public long[] values() {
            update();
            return histogram.getValues();
        }
    }

    public MetricMBean histogram(String url, boolean considerZeroes) {
        return new JmxHistogram(url, UPDATE_INTERVAL);
    }

    private class JmxTimer extends JmxMeter implements JmxTimerMBean {
        private Histogram histogram = new Histogram();

        public JmxTimer(String url, long interval) {
            super(url, interval);
        }

        @Override
        public void update(JsonObject obj) {
            // TODO: this is not atomic.
            super.update(obj.getJsonObject("meter"));
            histogram = new Histogram(obj.getJsonObject("hist"));
        }

        @Override
        public double getMin() {
            update();
            return toDuration(histogram.getMin());
        }

        @Override
        public double getMax() {
            update();
            return toDuration(histogram.getMax());
        }

        @Override
        public double getMean() {
            update();
            return toDuration(histogram.getMean());
        }

        @Override
        public double getStdDev() {
            update();
            return toDuration(histogram.getStdDev());
        }

        @Override
        public double get50thPercentile() {
            update();
            return toDuration(histogram.getValue(.5));
        }

        @Override
        public double get75thPercentile() {
            update();
            return toDuration(histogram.getValue(.75));
        }

        @Override
        public double get95thPercentile() {
            update();
            return toDuration(histogram.getValue(.95));
        }

        @Override
        public double get98thPercentile() {
            update();
            return toDuration(histogram.getValue(.98));
        }

        @Override
        public double get99thPercentile() {
            update();
            return toDuration(histogram.getValue(.99));
        }

        @Override
        public double get999thPercentile() {
            update();
            return toDuration(histogram.getValue(.999));
        }

        @Override
        public long[] values() {
            update();
            return histogram.getValues();
        }

        @Override
        public String getDurationUnit() {
            update();
            return DURATION_UNIT.toString().toLowerCase(Locale.US);
        }
    }

    public MetricMBean timer(String url) {
        return new JmxTimer(url, UPDATE_INTERVAL);
    }

    public interface MetricMBean {
    }

    public static interface JmxGaugeMBean extends MetricMBean {
        Object getValue();
    }

    public interface JmxHistogramMBean extends MetricMBean {
        long getCount();

        long getMin();

        long getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();
    }

    public interface JmxCounterMBean extends MetricMBean {
        long getCount();
    }

    public interface JmxMeterMBean extends MetricMBean {
        long getCount();

        double getMeanRate();

        double getOneMinuteRate();

        double getFiveMinuteRate();

        double getFifteenMinuteRate();

        String getRateUnit();
    }

    public interface JmxTimerMBean extends JmxMeterMBean {
        double getMin();

        double getMax();

        double getMean();

        double getStdDev();

        double get50thPercentile();

        double get75thPercentile();

        double get95thPercentile();

        double get98thPercentile();

        double get99thPercentile();

        double get999thPercentile();

        long[] values();

        String getDurationUnit();
    }
}
