package io.ray.runtime.metric;

import java.util.Map;

public interface Metric {
  // Sync metric with core worker stats for registry.
  // Metric data will be flushed into stats view data inside core worker immediately after
  // record is called.
  void record();

  // Update metric info for user.
  void update(double value);

  void update(double value, Map<TagKey, String> tags);

  void unregister();
}
