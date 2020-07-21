package io.ray.runtime.metric;


import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sum measurement is mapped to sum object in stats.
 * Property sum is used for storing transient sum for registry aggregation.
 */
public class Sum extends Metric {
  private double sum;

  public Sum(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = NativeMetric.registerSumNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0,"Count native pointer must not be 0.");
    this.sum = 0.0d;
  }

  @Override
  public void update(double value) {
    super.update(value);
    sum += value;
  }

  @Override
  public void update(double value, Map<TagKey, String> tags) {
    super.update(value, tags);
    sum += value;
  }

  @Override
  public void reset() {

  }

  public double getSum() {
    return sum;
  }
}
