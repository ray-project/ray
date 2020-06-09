package io.ray.api.call;

import io.ray.api.options.CallOptions;
import java.util.HashMap;
import java.util.Map;

public class TaskCallerBase<T extends TaskCallerBase<T>> {
  private Map<String, Double> resources = new HashMap<>();

  public T setResource(String key, Double value) {
    this.resources .put(key, value);
    return self();
  }

  public T setResources(Map<String, Double> resources) {
    this.resources.putAll(resources);
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected CallOptions createCallOptions() {
    return new CallOptions.Builder()
        .setResources(resources)
        .createCallOptions();
  }

}
