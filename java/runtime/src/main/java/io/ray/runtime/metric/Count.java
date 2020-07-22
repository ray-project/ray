package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;

public class Count extends Metric {

  private DoubleAdder count;

  public Count(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    count = new DoubleAdder();
    metricNativePointer = NativeMetric.registerCountNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Count native pointer must not be 0.");
  }

  @Override
  public void update(double value) {
    count.add(value);
  }

  @Override
  public void reset() {
    count.reset();
  }

  @Override
  public double getValue() {
    return getCount();
  }

  public double getCount() {
    return count.sum();
  }

  /**
   * @param delta add delta for counter
   */
  public void inc(double delta) {
    update(delta);
  }
}
