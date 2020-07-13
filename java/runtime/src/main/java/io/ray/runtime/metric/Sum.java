package io.ray.runtime.metric;


import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Sum extends Metric {
  public Sum(String name, String description, String unit, Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = registerSumNative(name, description, unit,
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0,"Count native pointer must not be 0.");
  }

  private native long registerSumNative(String name, String description,
                                          String unit, List<String> tagKeys);

}
