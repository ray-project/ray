package io.ray.runtime.metric;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Gauge metric for recording last value and mapping object from stats.
 */
public class Gauge extends Metric {

  private AtomicDouble value;

  public Gauge(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = NativeMetric.registerGaugeNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0, "Gauge native pointer must not be 0.");
    value = new AtomicDouble();
  }

  @Override
  public void reset() {
    value.set(0.0d);
  }

  /**
   * @return latest updating value.
   */
  @Override
  public double getValue() {
    return value.get();
  }

  /**
   * Update gauge value without tags.
   * Update metric info for user.
   *
   * @param value lastest value for updating
   */
  @Override
  public void update(double value) {
    this.value.set(value);
  }
}

