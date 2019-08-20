package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RECONSTRUCTION = 0;
  public static final int INFINITE_RECONSTRUCTIONS = (int) Math.pow(2, 30);
  private static final boolean DEFAULT_IS_DIRECT_CALL = "1"
      .equals(System.getenv("ACTOR_CREATION_OPTIONS_DEFAULT_IS_DIRECT_CALL"));

  public final int maxReconstructions;

  public final boolean isDirectCall;

  public final String jvmOptions;

  private ActorCreationOptions(Map<String, Double> resources, int maxReconstructions,
      boolean isDirectCall, String jvmOptions) {
    super(resources);
    this.maxReconstructions = maxReconstructions;
    this.isDirectCall = isDirectCall;
    this.jvmOptions = jvmOptions;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private int maxReconstructions = NO_RECONSTRUCTION;
    private boolean isDirectCall = DEFAULT_IS_DIRECT_CALL;
    private String jvmOptions = "";

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    public Builder setMaxReconstructions(int maxReconstructions) {
      this.maxReconstructions = maxReconstructions;
      return this;
    }

    public Builder setIsDirectCall(boolean isDirectCall) {
      this.isDirectCall = isDirectCall;
      return this;
    }

    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    public ActorCreationOptions createActorCreationOptions() {
      return new ActorCreationOptions(resources, maxReconstructions, isDirectCall, jvmOptions);
    }
  }

}
