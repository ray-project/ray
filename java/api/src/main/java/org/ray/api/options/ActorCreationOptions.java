package org.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {

  public static final int NO_RECONSTRUCTION = 0;
  public static final int INFINITE_RECONSTRUCTION = (int) Math.pow(2, 30);

  public final int maxReconstructions;

  public final String jvmOptions;

  public final int maxConcurrency;

  private ActorCreationOptions(Map<String, Double> resources, int maxReconstructions,
                               boolean useDirectCall, String jvmOptions, int maxConcurrency) {
    super(resources, useDirectCall);
    this.maxReconstructions = maxReconstructions;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private int maxReconstructions = NO_RECONSTRUCTION;
    private boolean useDirectCall = DEFAULT_USE_DIRECT_CALL;
    private String jvmOptions = null;
    private int maxConcurrency = 1;

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
    // public Builder setUseDirectCall(boolean useDirectCall) {
    //   this.useDirectCall = useDirectCall;
    //   return this;
    // }

    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    // The max number of concurrent calls to allow for this actor.
    //
    // This only works with direct actor calls. The max concurrency defaults to 1
    // for threaded execution. Note that the execution order is not guaranteed
    // when max_concurrency > 1.
    public Builder setMaxConcurrency(int maxConcurrency) {
      if (maxConcurrency <= 0) {
        throw new IllegalArgumentException("maxConcurrency must be greater than 0.");
      }

      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public ActorCreationOptions createActorCreationOptions() {
      return new ActorCreationOptions(
          resources, maxReconstructions, useDirectCall, jvmOptions, maxConcurrency);
    }
  }

}
