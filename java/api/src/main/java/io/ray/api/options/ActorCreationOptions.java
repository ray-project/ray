package io.ray.api.options;

import io.ray.api.Ray;
import io.ray.api.placementgroup.PlacementGroup;
import java.util.HashMap;
import java.util.Map;

/** The options for creating actor. */
public class ActorCreationOptions extends BaseTaskOptions {
  public final boolean global;
  public final String name;
  public final int maxRestarts;
  public final String jvmOptions;
  public final int maxConcurrency;
  public final PlacementGroup group;
  public final int bundleIndex;

  private ActorCreationOptions(
      boolean global,
      String name,
      Map<String, Double> resources,
      int maxRestarts,
      String jvmOptions,
      int maxConcurrency,
      PlacementGroup group,
      int bundleIndex) {
    super(resources);
    this.global = global;
    this.name = name;
    this.maxRestarts = maxRestarts;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
    this.group = group;
    this.bundleIndex = bundleIndex;
  }

  /** The inner class for building ActorCreationOptions. */
  public static class Builder {
    private boolean global;
    private String name;
    private Map<String, Double> resources = new HashMap<>();
    private int maxRestarts = 0;
    private String jvmOptions = null;
    private int maxConcurrency = 1;
    private PlacementGroup group;
    private int bundleIndex;

    /**
     * Set the actor name of a named actor. This named actor is only accessible from this job by
     * this name via {@link Ray#getActor(java.lang.String)}. If you want create a named actor that
     * is accessible from all jobs, use {@link Builder#setGlobalName(java.lang.String)} instead.
     *
     * @param name The name of the named actor.
     * @return self
     */
    public Builder setName(String name) {
      this.name = name;
      this.global = false;
      return this;
    }

    /**
     * Set the name of this actor. This actor will be accessible from all jobs by this name via
     * {@link Ray#getGlobalActor(java.lang.String)}. If you want to create a named actor that is
     * only accessible from this job, use {@link Builder#setName(java.lang.String)} instead.
     *
     * @param name The name of the named actor.
     * @return self
     */
    public Builder setGlobalName(String name) {
      this.name = name;
      this.global = true;
      return this;
    }

    /**
     * Set a custom resource requirement to reserve for the lifetime of this actor. This method can
     * be called multiple times. If the same resource is set multiple times, the latest quantity
     * will be used.
     *
     * @param resourceName resource name
     * @param resourceQuantity resource quantity
     * @return self
     */
    public Builder setResource(String resourceName, Double resourceQuantity) {
      this.resources.put(resourceName, resourceQuantity);
      return this;
    }

    /**
     * Set custom resource requirements to reserve for the lifetime of this actor. This method can
     * be called multiple times. If the same resource is set multiple times, the latest quantity
     * will be used.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    /**
     * This specifies the maximum number of times that the actor should be restarted when it dies
     * unexpectedly. The minimum valid value is 0 (default), which indicates that the actor doesn't
     * need to be restarted. A value of -1 indicates that an actor should be restarted indefinitely.
     *
     * @param maxRestarts max number of actor restarts
     * @return self
     */
    public Builder setMaxRestarts(int maxRestarts) {
      this.maxRestarts = maxRestarts;
      return this;
    }

    /**
     * Set the JVM options for the Java worker that this actor is running in.
     *
     * <p>Note, if this is set, this actor won't share Java worker with other actors or tasks.
     *
     * @param jvmOptions JVM options for the Java worker that this actor is running in.
     * @return self
     */
    public Builder setJvmOptions(String jvmOptions) {
      this.jvmOptions = jvmOptions;
      return this;
    }

    /**
     * Set the max number of concurrent calls to allow for this actor.
     *
     * <p>The max concurrency defaults to 1 for threaded execution. Note that the execution order is
     * not guaranteed when {@code max_concurrency > 1}.
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

    /**
     * Set the placement group to place this actor in.
     *
     * @param group The placement group of the actor.
     * @param bundleIndex The index of the bundle to place this actor in.
     * @return self
     */
    public Builder setPlacementGroup(PlacementGroup group, int bundleIndex) {
      this.group = group;
      this.bundleIndex = bundleIndex;
      return this;
    }

    public ActorCreationOptions build() {
      return new ActorCreationOptions(
          global, name, resources, maxRestarts, jvmOptions, maxConcurrency, group, bundleIndex);
    }
  }
}
