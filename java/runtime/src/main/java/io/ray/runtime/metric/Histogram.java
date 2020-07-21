package io.ray.runtime.metric;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Histogram measurement is mapped to histogram object in stats.
 * In order to reduce JNI calls overhead, a memory historical window is used
 * for storing transient value and we assume its max size is 100.
 */
public class Histogram extends Metric {

  private List<Double> histogramWindow;
  public static final int HISTOGRAM_WINDOW_SIZE = 100;

  public Histogram(String name, String description, String unit, List<Double> boundaries,
                   Map<TagKey, String> tags) {
    super(name, tags);
    metricNativePointer = NativeMetric.registerHistogramNative(name, description, unit,
      boundaries.stream().mapToDouble(Double::doubleValue).toArray(),
      tags.keySet().stream().map(TagKey::getTagKey).collect(Collectors.toList()));
    Preconditions.checkState(metricNativePointer != 0,
        "Histogram native pointer must not be 0.");
    histogramWindow = new ArrayList<>();
  }

  private void updateForWindow(double value) {
    if (histogramWindow.size() == HISTOGRAM_WINDOW_SIZE) {
      histogramWindow.remove(0);
    }
    histogramWindow.add(value);
  }

  @Override
  public void update(double value) {
    super.update(value);
    updateForWindow(value);
  }

  @Override
  public void update(double value, Map<TagKey, String> tags) {
    super.update(value, tags);
    updateForWindow(value);
  }

  @Override
  public void reset() {

  }

  public List<Double> getHistogramWindow() {
    return histogramWindow;
  }
}
