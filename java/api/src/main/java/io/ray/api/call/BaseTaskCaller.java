package io.ray.api.call;

import io.ray.api.options.CallOptions;
import java.util.Map;

/**
 * Base helper to call remote function.
 *
 * @param <T> The type of the concrete task caller
 */
public class BaseTaskCaller<T extends BaseTaskCaller<T>> {
  private CallOptions.Builder builder = new CallOptions.Builder();

  /**
   * @see CallOptions.Builder#setResource(java.lang.String, java.lang.Double)
   */
  public T setResource(String name, Double value) {
    builder.setResource(name, value);
    return self();
  }

  /**
   * @see CallOptions.Builder#setResources(java.util.Map)
   */
  public T setResources(Map<String, Double> resources) {
    builder.setResources(resources);
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

  protected CallOptions buildOptions() {
    return builder.build();
  }

}
