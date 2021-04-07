package io.ray.runtime.metric;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.stream.Collectors;

/** Count measurement is mapped to count object in stats and counts the number. */
public class Count extends Metric {

  private DoubleAdder count;

  public Count(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    count = new DoubleAdder();
    metricNativePointer =
        NativeMetric.registerCountNative(
            name,
            description,
            unit,
            tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Count native pointer must not be 0.");
  }

  @Override
  public void update(double value) {
    count.add(value);
    this.value.addAndGet(value);
  }

  @Override
  protected double getAndReset() {
    return count.sumThenReset();
  }

  public double getCount() {
    return this.value.get();
  }

  public void inc(double delta) {
    update(delta);
  }
}
