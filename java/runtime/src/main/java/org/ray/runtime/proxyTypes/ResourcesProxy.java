package org.ray.runtime.proxyTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ResourcesProxy {
  public List<String> keys;
  public List<Double> values;

  public ResourcesProxy(Map<String, Double> resources) {
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
