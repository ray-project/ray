package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.stream.Collectors;

public class Count extends Metric {

  private double count;

  public Count(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    count = 0.0d;
    metricNativePointer = NativeMetric.registerCountNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Count native pointer must not be 0.");
  }

  @Override
  public void update(double value) {
    super.update(value);
    count += value;
  }

  @Override
  public void update(double value, Map<TagKey, String> tags) {
    super.update(value, tags);
    count += value;
  }

  @Override
  public void reset() {

  }

  public double getCount() {
    return count;
  }

  /**
   * @param delta add delta for counter
   */
  public void inc(double delta) {
    update(delta);
  }
}
