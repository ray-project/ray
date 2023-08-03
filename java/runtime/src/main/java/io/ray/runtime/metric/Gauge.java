package io.ray.runtime.metric;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.stream.Collectors;

/** Gauge measurement is mapped to gauge object in stats and is recording the last value. */
public class Gauge extends Metric {

  @Deprecated
  public Gauge(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer =
        NativeMetric.registerGaugeNative(
            name,
            description,
            unit,
            tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Gauge native pointer must not be 0.");
  }

  public Gauge(String name, String description, Map<String, String> tags) {
    this(name, description, "", TagKey.tagsFromMap(tags));
  }

  public double getValue() {
    return value.doubleValue();
  }

  @Override
  protected double getAndReset() {
    return value.doubleValue();
  }

  @Override
  public void update(double value) {
    this.value.set(value);
  }
}
