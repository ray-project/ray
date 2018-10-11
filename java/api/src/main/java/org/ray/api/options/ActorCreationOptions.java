package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RECONSTRUCTION = 0;
  public static final int INFINITE_RECONSTRUCTIONS = Integer.MAX_VALUE;

  public final int maxReconstructions;

  public ActorCreationOptions() {
    super();
    this.maxReconstructions = NO_RECONSTRUCTION;
  }

  public ActorCreationOptions(Map<String, Double> resources) {
    super(resources);
    this.maxReconstructions = NO_RECONSTRUCTION;
  }

  public ActorCreationOptions(Map<String, Double> resources, int maxReconstructions) {
    super(resources);
    this.maxReconstructions = maxReconstructions;
  }
}
