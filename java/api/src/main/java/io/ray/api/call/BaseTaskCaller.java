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
   * Set a name for this task.
   *
   * @param name task name
   * @return self
   * @see CallOptions.Builder#setName(java.lang.String)
   */
  public T setName(String name) {
    builder.setName(name);
    return self();
  }

  /**
   * Set a custom resource requirement for resource {@code name}. This method can be called multiple
   * times. If the same resource is set multiple times, the latest quantity will be used.
   *
   * @param name resource name
   * @param value resource capacity
   * @return self
   * @see CallOptions.Builder#setResource(java.lang.String, java.lang.Double)
   */
  public T setResource(String name, Double value) {
    builder.setResource(name, value);
    return self();
  }

  /**
   * Set custom requirements for multiple resources. This method can be called multiple times. If
   * the same resource is set multiple times, the latest quantity will be used.
   *
   * @param resources requirements for multiple resources.
   * @return self
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
