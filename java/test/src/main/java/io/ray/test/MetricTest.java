package io.ray.test;

import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.Sum;
import io.ray.runtime.metric.TagKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MetricTest extends BaseTest {

  boolean doubleEqual(double value, double other) {
    return value <= other + 1e-5 && value >= other - 1e-5;
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
    Assert.assertTrue(doubleEqual(count.getValue(), 20.0));
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
    Assert.assertTrue(doubleEqual(sum.getValue(), 20.0));
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
}
