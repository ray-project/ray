package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RECONSTRUCTION = 0;
  public static final int INFINITE_RECONSTRUCTIONS = (int) Math.pow(2, 30);
  // DO NOT set this environment variable. It's only used for test purposes.
  // Please use `setUseDirectCall` instead.
  public static final boolean DEFAULT_USE_DIRECT_CALL = "1"
      .equals(System.getenv("ACTOR_CREATION_OPTIONS_DEFAULT_USE_DIRECT_CALL"));

  public final int maxReconstructions;

  public final boolean useDirectCall;

  public final String jvmOptions;

  private ActorCreationOptions(Map<String, Double> resources, int maxReconstructions,
      boolean useDirectCall, String jvmOptions) {
    super(resources);
    this.maxReconstructions = maxReconstructions;
    this.useDirectCall = useDirectCall;
    this.jvmOptions = jvmOptions;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private int maxReconstructions = NO_RECONSTRUCTION;
    private boolean useDirectCall = DEFAULT_USE_DIRECT_CALL;
    private String jvmOptions = null;

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    public Builder setMaxReconstructions(int maxReconstructions) {
      this.maxReconstructions = maxReconstructions;
      return this;
    }

    // Since direct call is not fully supported yet (see issue #5559),
    // users are not allowed to set the option to true.
    // TODO (kfstorm): uncomment when direct call is ready.
//    public Builder setUseDirectCall(boolean useDirectCall) {
//      this.useDirectCall = useDirectCall;
//      return this;
//    }

    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    public ActorCreationOptions createActorCreationOptions() {
      return new ActorCreationOptions(resources, maxReconstructions, useDirectCall, jvmOptions);
    }
  }

}
