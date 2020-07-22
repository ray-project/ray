package io.ray.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.MetricConfig;
import io.ray.runtime.metric.RayMetrics;
import io.ray.runtime.metric.Sum;
import io.ray.runtime.metric.TagKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricTest extends BaseTest {

  boolean doubleEqual(double value, double other) {
    return value <= other + 1e-5 && value >= other - 1e-5;
  }

  private MetricConfig initRayMetrics(long timeIntervalMs,
                                      int threadPoolSize,
                                      long shutdownWaitTimeMs) {
    MetricConfig config = MetricConfig.builder()
      .timeIntervalMs(timeIntervalMs)
      .threadPoolSize(threadPoolSize)
      .shutdownWaitTimeMs(shutdownWaitTimeMs)
      .create();
    RayMetrics.init(config);
    return config;
  }

  private Gauge registerGauge() {
    return RayMetrics.gauge()
      .name("metric_gauge")
      .description("gauge")
      .unit("")
      .tags(ImmutableMap.of("tag1", "value1"))
      .register();
  }

  private Count registerCount() {
    return RayMetrics.count()
      .name("metric_count")
      .description("counter")
      .unit("1pc")
      .tags(ImmutableMap.of("tag1", "value1", "count_tag", "default"))
      .register();
  }

  private Sum registerSum() {
    return RayMetrics.sum()
      .name("metric_sum")
      .description("sum")
      .unit("1pc")
      .tags(ImmutableMap.of("tag1", "value1", "sum_tag", "default"))
      .register();
  }

  private Histogram registerHistogram() {
    return RayMetrics.histogram()
      .name("metric_histogram")
      .description("histogram")
      .unit("1pc")
      .boundaries(ImmutableList.of(10.0, 15.0, 20.0))
      .tags(ImmutableMap.of("tag1", "value1", "histogram_tag", "default"))
      .register();
  }

  @Test
  public void testAddGauge() {
    TestUtils.skipTestUnderSingleProcess();
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");

    Gauge gauge = new Gauge("metric1", "", "", tags);
    gauge.update(2);
    gauge.record();
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    gauge.unregister();
  }

  @Test
  public void testAddCount() {
    TestUtils.skipTestUnderSingleProcess();
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");
    tags.put(new TagKey("count_tag"), "default");

    Count count = new Count("metric_count", "counter", "1pc", tags);
    count.inc(10.0);
    count.inc(20.0);
    count.record();
    Assert.assertTrue(doubleEqual(count.getCount(), 30.0));
  }

  @Test
  public void testAddSum() {
    TestUtils.skipTestUnderSingleProcess();
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");
    tags.put(new TagKey("sum_tag"), "default");

    Sum sum = new Sum("metric_sum", "sum", "sum", tags);
    sum.update(10.0);
    sum.update(20.0);
    sum.record();
    Assert.assertTrue(doubleEqual(sum.getSum(), 30.0));
  }

  @Test
  public void testAddHistogram() {
    TestUtils.skipTestUnderSingleProcess();
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");
    tags.put(new TagKey("histogram_tag"), "default");
    List<Double> boundaries = new ArrayList<>();
    boundaries.add(10.0);
    boundaries.add(15.0);
    boundaries.add(12.0);
    Histogram histogram = new Histogram("metric_histogram", "histogram", "1pc",
      boundaries, tags);
    for (int i = 1; i <= 200; ++i) {
      histogram.update(i * 1.0d);
      histogram.record();
    }
    List<Double> window = histogram.getHistogramWindow();
    for (int i = 0; i < Histogram.HISTOGRAM_WINDOW_SIZE; ++i) {
      Assert.assertTrue(doubleEqual(i + 101.0d, window.get(i)));
    }
  }

  @Test
  public void testRegisterGauge() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    Gauge gauge = registerGauge();

    gauge.update(2.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    TimeUnit.MILLISECONDS.sleep(MetricConfig.DEFAULT_CONFIG.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 0.0));
    gauge.update(5.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 5.0));
  }

  @Test
  public void testRegisterCount() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    Count count = registerCount();

    count.inc(10.0);
    count.inc(20.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 30.0));
    TimeUnit.MILLISECONDS.sleep(MetricConfig.DEFAULT_CONFIG.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(count.getCount(), 0.0));
    count.inc(1.0);
    count.inc(2.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 3.0));
  }

  @Test
  public void testRegisterSum() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    Sum sum = registerSum();

    sum.update(10.0);
    sum.update(20.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 30.0));
    TimeUnit.MILLISECONDS.sleep(MetricConfig.DEFAULT_CONFIG.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(sum.getSum(), 0.0));
    sum.update(1.0);
    sum.update(2.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 3.0));
  }

  @Test
  public void testRegisterHistogram() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    Histogram histogram = registerHistogram();

    for (int i = 1; i <= 200; ++i) {
      histogram.update(i * 1.0d);
    }
    Assert.assertTrue(doubleEqual(histogram.getValue(), 200.0));
    List<Double> window = histogram.getHistogramWindow();
    for (int i = 0; i < Histogram.HISTOGRAM_WINDOW_SIZE; ++i) {
      Assert.assertTrue(doubleEqual(i + 101.0d, window.get(i)));
    }
    TimeUnit.MILLISECONDS.sleep(MetricConfig.DEFAULT_CONFIG.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(histogram.getValue(), 0.0));
  }

  @Test
  public void testRegisterGaugeWithConfig() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    MetricConfig config = initRayMetrics(3000L, 1, 1000L);
    Gauge gauge = registerGauge();

    gauge.update(2.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 2.0));
    TimeUnit.MILLISECONDS.sleep(config.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 0.0));
    gauge.update(5.0);
    Assert.assertTrue(doubleEqual(gauge.getValue(), 5.0));
  }

  @Test
  public void testRegisterCountWithConfig() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    MetricConfig config = initRayMetrics(3000L, 1, 1000L);
    Count count = registerCount();

    count.inc(10.0);
    count.inc(20.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 30.0));
    TimeUnit.MILLISECONDS.sleep(config.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(count.getCount(), 0.0));
    count.inc(1.0);
    count.inc(2.0);
    Assert.assertTrue(doubleEqual(count.getCount(), 3.0));
  }

  @Test
  public void testRegisterSumWithConfig() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    MetricConfig config = initRayMetrics(3000L, 1, 1000L);
    Sum sum = registerSum();

    sum.update(10.0);
    sum.update(20.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 30.0));
    TimeUnit.MILLISECONDS.sleep(config.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(sum.getSum(), 0.0));
    sum.update(1.0);
    sum.update(2.0);
    Assert.assertTrue(doubleEqual(sum.getSum(), 3.0));
  }

  @Test
  public void testRegisterHistogramWithConfig() throws InterruptedException {
    TestUtils.skipTestUnderSingleProcess();
    MetricConfig config = initRayMetrics(3000L, 1, 1000L);
    Histogram histogram = registerHistogram();

    for (int i = 1; i <= 200; ++i) {
      histogram.update(i * 1.0d);
    }
    Assert.assertTrue(doubleEqual(histogram.getValue(), 200.0));
    List<Double> window = histogram.getHistogramWindow();
    for (int i = 0; i < Histogram.HISTOGRAM_WINDOW_SIZE; ++i) {
      Assert.assertTrue(doubleEqual(i + 101.0d, window.get(i)));
    }
    TimeUnit.MILLISECONDS.sleep(config.timeIntervalMs() + 1000L);
    Assert.assertTrue(doubleEqual(histogram.getValue(), 0.0));
  }

}
