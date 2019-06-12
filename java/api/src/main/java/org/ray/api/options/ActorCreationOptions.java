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

  public final String workerStartingPrefix;

  public final String workerStartingSuffix;

  private ActorCreationOptions(Map<String, Double> resources,
                               int maxReconstructions,
                               String workerStartingPrefix,
                               String workerStartingSuffix) {
    super(resources);
    this.maxReconstructions = maxReconstructions;
    this.workerStartingPrefix = workerStartingPrefix;
    this.workerStartingSuffix = workerStartingSuffix;
  }

  /**
   *  The inner class for building ActorCreationOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private int maxReconstructions = NO_RECONSTRUCTION;
    private String workerStartingPrefix = "";
    private String workerStartingSuffix = "";

    public Builder setResources(Map<String, Double> resources) {
      this.resources = resources;
      return this;
    }

    public Builder setMaxReconstructions(int maxReconstructions) {
      this.maxReconstructions = maxReconstructions;
      return this;
    }

    public Builder setWorkerStartingPrefix(String workerStartingPrefix) {
      this.workerStartingPrefix = workerStartingPrefix;
      return this;
    }

    public Builder setWorkerStartingSuffix(String workerStartingSuffix) {
      this.workerStartingSuffix = workerStartingSuffix;
      return this;
    }

    public ActorCreationOptions createActorCreationOptions() {
      return new ActorCreationOptions(resources, maxReconstructions,
          workerStartingPrefix, workerStartingSuffix);
    }
  }

}
