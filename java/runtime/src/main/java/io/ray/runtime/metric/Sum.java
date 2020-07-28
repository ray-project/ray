package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

import java.util.Map;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;

/**
 * Sum measurement is mapped to sum object in stats.
 * Property sum is used for storing transient sum for registry aggregation.
 */
public class Sum extends Metric {

  private DoubleAdder sum;

  public Sum(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = NativeMetric.registerSumNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Count native pointer must not be 0.");
    this.sum = new DoubleAdder();
  }

  @Override
  public void update(double value) {
    sum.add(value);
    this.value.addAndGet(value);
  }

  @Override
  protected double getAndReset() {
    return sum.sumThenReset();
  }

  public double getSum() {
    return value.get();
  }
}
