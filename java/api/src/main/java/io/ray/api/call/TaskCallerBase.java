package io.ray.api.call;

import io.ray.api.options.CallOptions;
import java.util.Map;

public class TaskCallerBase<T extends TaskCallerBase<T>> {
  private CallOptions.Builder builder = new CallOptions.Builder();

  public T setResource(String key, Double value) {
    builder.setResource(key, value);
    return self();
  }

  public T setResources(Map<String, Double> resources) {
    builder.setResources(resources);
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected CallOptions createCallOptions() {
    return builder.createCallOptions();
  }

}
