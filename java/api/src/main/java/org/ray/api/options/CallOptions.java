package org.ray.api.options;

import java.util.Map;

/**
 * The options for RayCall.
 */
public class CallOptions extends BaseTaskOptions {

  public CallOptions() {
    super();
  }

  public CallOptions(Map<String, Double> resources) {
    super(resources);
  }

}
