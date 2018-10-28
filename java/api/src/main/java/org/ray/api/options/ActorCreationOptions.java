package org.ray.api.options;

import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public ActorCreationOptions() {
    super();
  }

  public ActorCreationOptions(Map<String, Double> resources) {
    super(resources);
  }

}
