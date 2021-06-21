package io.ray.runtime.metric;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AtomicDouble;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Class metric is mapped to stats metric object in core worker. it must be in categories set
 * [Gague, Count, Sum, Histogram].
 */
public abstract class Metric {
  protected String name;

  protected AtomicDouble value;

  // Native pointer mapping to gauge object of stats.
  protected long metricNativePointer = 0L;

  protected Map<TagKey, String> tags;

  public Metric(String name, Map<TagKey, String> tags) {
    Preconditions.checkNotNull(tags, "Metric tags map must not be null.");
    Preconditions.checkNotNull(name, "Metric name must not be null.");
    this.name = name;
    this.tags = tags;
    this.value = new AtomicDouble();
  }

  // Sync metric with core worker stats for registry.
  // Metric data will be flushed into stats view data inside core worker immediately after
  // record is called.

  /** Flush records to stats in last aggregator. */
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
    NativeMetric.recordNative(
        metricNativePointer,
        getAndReset(),
        nativeTagKeyList.stream().map(TagKey::getTagKey).collect(Collectors.toList()),
        tagValues);
  }

  /**
   * Get the value to record and then reset.
   *
   * @return latest updating value.
   */
  protected abstract double getAndReset();

  /**
   * Update gauge value without tags. Update metric info for user.
   *
   * @param value latest value for updating
   */
  public abstract void update(double value);

  /**
   * Update gauge value with dynamic tag values.
   *
   * @param value latest value for updating
   * @param tags tag map
   */
  public void update(double value, Map<TagKey, String> tags) {
    update(value);
    this.tags = tags;
  }

  /** Deallocate object from stats and reset native pointer in null. */
  public void unregister() {
    if (0 != metricNativePointer) {
      NativeMetric.unregisterMetricNative(metricNativePointer);
    }
    metricNativePointer = 0;
  }
}
