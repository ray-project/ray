package org.ray.runtime.nativeTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NativeTaskOptions {
  public int numReturns;
  public List<String> resourceKeys;
  public List<Double> resourceValues;

  public NativeTaskOptions(int numReturns, Map<String, Double> resources) {
    this.numReturns = numReturns;
    resourceKeys = new ArrayList<>(resources.size());
    resourceValues = new ArrayList<>(resources.size());
    for (Map.Entry<String, Double> entry : resources.entrySet()) {
      resourceKeys.add(entry.getKey());
      resourceValues.add(entry.getValue());
    }
  }
}
