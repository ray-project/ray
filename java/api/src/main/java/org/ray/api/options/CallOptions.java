package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for RayCall.
 */
public class CallOptions extends BaseTaskOptions {

  private CallOptions(Map<String, Double> resources, boolean useDirectCall) {
    super(resources, useDirectCall);
  }

  /**
   * This inner class for building CallOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private boolean useDirectCall = DEFAULT_USE_DIRECT_CALL;

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    // Since direct call is not fully supported yet (see issue #5559),
    // users are not allowed to set the option to true.
    // TODO (kfstorm): uncomment when direct call is ready.
    // public Builder setUseDirectCall(boolean useDirectCall) {
    //   this.useDirectCall = useDirectCall;
    //   return this;
    // }

    public CallOptions createCallOptions() {
      return new CallOptions(resources, useDirectCall);
    }
  }
}
