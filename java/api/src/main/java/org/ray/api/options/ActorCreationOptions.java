package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RECONSTRUCTION = 0;
  public static final int INFINITE_RECONSTRUCTIONS = (int) Math.pow(2, 30);

  public final int maxReconstructions;

  private ActorCreationOptions(Builder builder) {
    super(builder.resources);
    this.maxReconstructions = builder.maxReconstructions;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {
    private Map<String, Double> resources = new HashMap<>();

    private int maxReconstructions = NO_RECONSTRUCTION;

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    public Builder setMaxReconstructions(int maxReconstructions) {
      this.maxReconstructions = maxReconstructions;
      return this;
    }

    public ActorCreationOptions build() {
      return new ActorCreationOptions(this);
    }
  }

}
