package io.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for RayCall.
 */
public class CallOptions extends BaseTaskOptions {

  private CallOptions(Map<String, Double> resources) {
    super(resources);
  }

  /**
   * This inner class for building CallOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();

    /**
     * Set a custom resource requirement for resource {@code name}.
     * This method can be called multiple times. If the same resource is set multiple times,
     * the latest quantity will be used.
     *
     * @param name resource name
     * @param value resource capacity
     * @return self
     */
    public Builder setResource(String name, Double value) {
      this.resources.put(name, value);
      return this;
    }

    /**
     * Set custom requirements for multiple resources.
     * This method can be called multiple times. If the same resource is set multiple times,
     * the latest quantity will be used.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    public CallOptions build() {
      return new CallOptions(resources);
    }
  }
}
