package io.ray.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.MetricConfig;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.metric.Sum;
import io.ray.runtime.metric.TagKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class MetricTest extends BaseTest {

  boolean doubleEqual(double value, double other) {
    return value <= other + 1e-5 && value >= other - 1e-5;
  }

  private MetricConfig initRayMetrics(
      long timeIntervalMs, int threadPoolSize, long shutdownWaitTimeMs) {
    MetricConfig config =
        MetricConfig.builder()
            .timeIntervalMs(timeIntervalMs)
            .threadPoolSize(threadPoolSize)
            .shutdownWaitTimeMs(shutdownWaitTimeMs)
            .create();
    Metrics.init(config);
    return config;
  }

  private Gauge registerGauge() {
    return Metrics.gauge()
        .name("metric_gauge")
        .description("gauge")
        .unit("")
        .tags(ImmutableMap.of("tag1", "value1"))
        .register();
  }

  private Count registerCount() {
    return Metrics.count()
        .name("metric_count")
        .description("counter")
        .unit("1pc")
        .tags(ImmutableMap.of("tag1", "value1", "count_tag", "default"))
        .register();
  }

  private Sum registerSum() {
    return Metrics.sum()
        .name("metric_sum")
        .description("sum")
        .unit("1pc")
        .tags(ImmutableMap.of("tag1", "value1", "sum_tag", "default"))
        .register();
  }

  private Histogram registerHistogram() {
    return Metrics.histogram()
        .name("metric_histogram")
        .description("histogram")
        .unit("1pc")
        .boundaries(ImmutableList.of(10.0, 15.0, 20.0))
        .tags(ImmutableMap.of("tag1", "value1", "histogram_tag", "default"))
        .register();
  }

  @AfterMethod
  public void maybeShutdownMetrics() {
    Metrics.shutdown();
  }

  public void testAddGauge() {
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");

    Gauge gauge = new Gauge("metric1", "", "", tags);
    gauge.update(2);
    gauge.record();
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    gauge.unregister();
  }

  public void testAddGaugeWithTagMap() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    Gauge gauge = new Gauge("metric1", "", tags);
    gauge.update(2);
    gauge.record();
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    gauge.unregister();
  }

  public void testAddCount() {
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");
    tags.put(new TagKey("count_tag"), "default");

    Count count = new Count("metric_count", "counter", "1pc", tags);
    count.inc(10.0);
    count.inc(20.0);
    count.record();
    Assert.assertTrue(doubleEqual(count.getCount(), 30.0));
  }

  public void testAddSum() {
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");
    tags.put(new TagKey("sum_tag"), "default");

    Sum sum = new Sum("metric_sum", "sum", "sum", tags);
    sum.update(10.0);
    sum.update(20.0);
    sum.record();
    Assert.assertTrue(doubleEqual(sum.getSum(), 30.0));
  }

  public void testAddHistogram() {
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");
    tags.put(new TagKey("histogram_tag"), "default");
    List<Double> boundaries = new ArrayList<>();
    boundaries.add(10.0);
    boundaries.add(12.0);
    boundaries.add(15.0);
    Histogram histogram = new Histogram("metric_histogram", "histogram", "1pc", boundaries, tags);
    for (int i = 1; i <= 200; ++i) {
      histogram.update(i * 1.0d);
    }
    Assert.assertTrue(doubleEqual(200.0d, histogram.getValue()));
    List<Double> window = histogram.getHistogramWindow();
    for (int i = 0; i < Histogram.HISTOGRAM_WINDOW_SIZE; ++i) {
      Assert.assertTrue(doubleEqual(i + 101.0d, window.get(i)));
    }
    histogram.record();
    Assert.assertTrue(doubleEqual(200.0d, histogram.getValue()));
    Assert.assertEquals(window.size(), 0);
  }

  public void testRegisterGauge() throws InterruptedException {
    Gauge gauge = registerGauge();

    gauge.update(2.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    gauge.update(5.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 5.0));
  }

  public void testRegisterCount() throws InterruptedException {
    Count count = registerCount();

    count.inc(10.0);
    count.inc(20.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 30.0));
    count.inc(1.0);
    count.inc(2.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 33.0));
  }

  public void testRegisterSum() throws InterruptedException {
    Sum sum = registerSum();

    sum.update(10.0);
    sum.update(20.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 30.0));
    sum.update(1.0);
    sum.update(2.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 33.0));
  }

  public void testRegisterHistogram() throws InterruptedException {
    Histogram histogram = registerHistogram();

    for (int i = 1; i <= 200; ++i) {
      histogram.update(i * 1.0d);
    }
    Assert.assertTrue(doubleEqual(histogram.getValue(), 200.0d));
    List<Double> window = histogram.getHistogramWindow();
    for (int i = 0; i < Histogram.HISTOGRAM_WINDOW_SIZE; ++i) {
      Assert.assertTrue(doubleEqual(i + 101.0d, window.get(i)));
    }
    Assert.assertTrue(doubleEqual(histogram.getValue(), 200.0d));
  }

  public void testRegisterGaugeWithConfig() throws InterruptedException {
    initRayMetrics(2000L, 1, 1000L);
    Gauge gauge = registerGauge();

    gauge.update(2.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    gauge.update(5.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 5.0));
  }

  public void testRegisterCountWithConfig() throws InterruptedException {
    initRayMetrics(2000L, 1, 1000L);
    Count count = registerCount();

    count.inc(10.0);
    count.inc(20.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 30.0));
    count.inc(1.0);
    count.inc(2.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 33.0));
  }

  public void testRegisterSumWithConfig() throws InterruptedException {
    initRayMetrics(2000L, 1, 1000L);
    Sum sum = registerSum();

    sum.update(10.0);
    sum.update(20.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 30.0));
    sum.update(1.0);
    sum.update(2.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 33.0));
  }

  public void testRegisterHistogramWithConfig() throws InterruptedException {
    initRayMetrics(2000L, 1, 1000L);
    Histogram histogram = registerHistogram();

    for (int i = 1; i <= 200; ++i) {
      histogram.update(i * 1.0d);
    }
    Assert.assertTrue(doubleEqual(histogram.getValue(), 200.0d));
    List<Double> window = histogram.getHistogramWindow();
    for (int i = 0; i < Histogram.HISTOGRAM_WINDOW_SIZE; ++i) {
      Assert.assertTrue(doubleEqual(i + 101.0d, window.get(i)));
    }
    Assert.assertTrue(doubleEqual(histogram.getValue(), 200.0d));
  }
}
