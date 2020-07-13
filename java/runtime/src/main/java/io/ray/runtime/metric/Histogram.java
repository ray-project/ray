package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Histogram extends Metric {

  public Histogram(String name, String description, String unit, List<Double> boundaries,
                   Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = registerHistogramNative(name, description, unit,
      boundaries.stream().mapToDouble(Double::doubleValue).toArray(),
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0,
        "Histogram native pointer must not be 0.");
  }

  private native long registerHistogramNative(String name, String description,
                                          String unit, double[] boundaries, List<String> tagKeys);

}
