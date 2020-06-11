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

    /**
     * Set a custom resource requirement to reserve for the lifetime of this actor.
     * This method can be called multiple times. If the same resource is set multiple times,
     * the latest quantity will be used.
     *
     * <p>By using custom resource, the user can implement virtually any actor placement policy.
     *
     * @param name resource name
     * @param value resource capacity
     * @return self
     */
    public Builder setResource(String name, Double value) {
      this.resources.put(name, value);
      return this;
    }

    /**
     * Set custom resource requirements to reserve for the lifetime of this actor.
     * This method can be called multiple times. If the same resource is set multiple times,
     * the latest quantity will be used.
     *
     * <p>By using custom resources, the user can implement virtually any actor placement policy.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    /**
     * Set max number of actor restarts when actor failed.
     *
     * @param maxRestarts max number of actor restarts
     * @return self
     */
    public Builder setMaxRestarts(int maxRestarts) {
      this.maxRestarts = maxRestarts;
      return this;
    }

    /**
     * Set java worker jvm start options
     *
     * @param jvmOptions java worker jvm start options
     * @return self
     */
    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    /**
     * Set the max number of concurrent calls to allow for this actor.
     *
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
