package io.ray.api.options;

import java.util.HashMap;
import java.util.Map;

/**
 * The options for creating actor.
 */
public class ActorCreationOptions extends BaseTaskOptions {
  public final int maxRestarts;

  public final String jvmOptions;

  public final int maxConcurrency;

  private ActorCreationOptions(Map<String, Double> resources, int maxRestarts,
                               String jvmOptions, int maxConcurrency) {
    super(resources);
    this.maxRestarts = maxRestarts;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
  }

  /**
   * The inner class for building ActorCreationOptions.
   */
  public static class Builder {

    private Map<String, Double> resources = new HashMap<>();
    private int maxRestarts = 0;
    private String jvmOptions = null;
    private int maxConcurrency = 1;

    public Builder setResource(String key, Double value) {
      this.resources.put(key, value);
      return this;
    }

    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    public Builder setMaxRestarts(int maxRestarts) {
      this.maxRestarts = maxRestarts;
      return this;
    }

    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    /**
     * The max concurrency defaults to 1 for threaded execution.
     * Note that the execution order is not guaranteed when max_concurrency > 1.
     *
     * @param maxConcurrency The max number of concurrent calls to allow for this actor.
     * @return self
     */
    public Builder setMaxConcurrency(int maxConcurrency) {
      if (maxConcurrency <= 0) {
        throw new IllegalArgumentException("maxConcurrency must be greater than 0.");
      }

      this.maxConcurrency = maxConcurrency;
      return this;
    }

    public ActorCreationOptions build() {
      return new ActorCreationOptions(
          resources, maxRestarts, jvmOptions, maxConcurrency);
    }
  }

}
