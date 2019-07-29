package org.ray.runtime.nativeTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NativeResources {
  public List<String> keys;
  public List<Double> values;

  public NativeResources(Map<String, Double> resources) {
    keys = new ArrayList<>();
    values = new ArrayList<>();
    if (resources != null) {
      for (Map.Entry<String, Double> entry : resources.entrySet()) {
        keys.add(entry.getKey());
        values.add(entry.getValue());
      }
    }
  }
}
