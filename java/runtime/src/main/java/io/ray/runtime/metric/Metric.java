package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class metric is mapped to stats metric object in core worker.
 * it must be in categories set [Gague, Count, Sum, Histogram].
 */
public abstract class Metric {
  protected String name;

  protected double value;
  // Native pointer mapping to gauge object of stats.
  protected long metricNativePointer = 0L;

  protected Map<TagKey, String> tags;

  public Metric(String name, Map<TagKey, String> tags) {
    Preconditions.checkNotNull(tags, "Metric tags map must not be null.");
    Preconditions.checkNotNull(name, "Metric name must not be null.");
    this.name = name;
    this.tags = tags;
    this.value = 0.0d;
  }

  // Sync metric with core worker stats for registry.
  // Metric data will be flushed into stats view data inside core worker immediately after
  // record is called.
  /**
   * Flush records to stats in last aggregator.
   */
  public void record() {
    Preconditions.checkState(metricNativePointer != 0, "Metric native pointer must not be 0.");
    // Get tag key list from map;
    List<TagKey> nativeTagKeyList = new ArrayList<>();
    List<String> tagValues = new ArrayList<>();
    for (Map.Entry<TagKey, String> entry : tags.entrySet()) {
      nativeTagKeyList.add(entry.getKey());
      tagValues.add(entry.getValue());
    }
    // Get tag value list from map;
    NativeMetric.recordNative(metricNativePointer, value, nativeTagKeyList.stream()
        .map(TagKey::getTagKey).collect(Collectors.toList()), tagValues);
  }

  /** Update gauge value without tags.
   * Update metric info for user.
   * @param value lastest value for updating
   */
  public void update(double value) {
    this.value = value;

  }

  /** Update gauge value with dynamic tag values.
   * @param value lastest value for updating
   * @param tags tag map
   */
  public void update(double value, Map<TagKey, String> tags) {
    this.value = value;
    this.tags = tags;
  }

  /**
   * Deallocate object from stats and reset native pointer in null.
   */
  public void unregister() {
    if (0 != metricNativePointer) {
      NativeMetric.unregisterMetricNative(metricNativePointer);
    }
    metricNativePointer = 0;
  }

  /**
   * @return lastest updating value.
   */
  public double getValue() {
    return value;
  }

  /**
   * It's abstract method for each metric measurements, so metric registry can store transient
   * value and aggregate historical data for flushing.
   */
  public abstract void reset();
}
