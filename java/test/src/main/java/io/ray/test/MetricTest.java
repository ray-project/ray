package io.ray.test;

import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.TagKey;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class MetricTest extends BaseTest {

  @Test
  public void testAddGauge() {
    Map<TagKey, String> tags = new HashMap<>();
    tags.put(new TagKey("tag1"), "value1");

    Gauge gauge = new Gauge("metric1", "", "", tags);
    gauge.update(2);
    gauge.record();
    gauge.unregister();
  }
}
