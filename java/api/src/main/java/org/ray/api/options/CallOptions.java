package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for RayCall.
 */
public class CallOptions extends BaseTaskOptions {

  private CallOptions(Builder builder) {
    super(builder.resources);
  }

  /**
   * This inner class for building CallOptions.
   */
  public static class Builder {
    private Map<String, Double> resources = new HashMap<>();

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    public CallOptions build() {
      return new CallOptions(this);
    }
  }
}
