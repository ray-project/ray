package io.ray.api.options;

import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.placementgroup.PlacementGroup;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The options for creating actor. */
public class ActorCreationOptions extends BaseTaskOptions {
  public final String name;
  public ActorLifetime lifetime;
  public final int maxRestarts;
  public final List<String> jvmOptions;
  public final int maxConcurrency;
  public final PlacementGroup group;
  public final int bundleIndex;
  public final List<ConcurrencyGroup> concurrencyGroups;
  public final int maxPendingCalls;

  private ActorCreationOptions(
      String name,
      ActorLifetime lifetime,
      Map<String, Double> resources,
      int maxRestarts,
      List<String> jvmOptions,
      int maxConcurrency,
      PlacementGroup group,
      int bundleIndex,
      List<ConcurrencyGroup> concurrencyGroups,
      int maxPendingCalls) {
    super(resources);
    this.name = name;
    this.lifetime = lifetime;
    this.maxRestarts = maxRestarts;
    this.jvmOptions = jvmOptions;
    this.maxConcurrency = maxConcurrency;
    this.group = group;
    this.bundleIndex = bundleIndex;
    this.concurrencyGroups = concurrencyGroups;
    this.maxPendingCalls = maxPendingCalls;
  }

  /** The inner class for building ActorCreationOptions. */
  public static class Builder {
    private String name;
    private ActorLifetime lifetime = null;
    private Map<String, Double> resources = new HashMap<>();
    private int maxRestarts = 0;
    private List<String> jvmOptions = new ArrayList<>();
    private int maxConcurrency = 1;
    private PlacementGroup group;
    private int bundleIndex;
    private List<ConcurrencyGroup> concurrencyGroups = new ArrayList<>();
    private int maxPendingCalls = -1;

    /**
     * Set the actor name of a named actor. This named actor is accessible in this namespace by this
     * name via {@link Ray#getActor(java.lang.String)} and in other namespaces via {@link
     * Ray#getActor(java.lang.String, java.lang.String)}.
     *
     * @param name The name of the named actor.
     * @return self
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /** Declare the lifetime of this actor. */
    public Builder setLifetime(ActorLifetime lifetime) {
      this.lifetime = lifetime;
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
    public Builder setJvmOptions(List<String> jvmOptions) {
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
     * Set the max number of pending calls allowed on the actor handle. When this value is exceeded,
     * ray.exceptions.PendingCallsLimitExceededException will be thrown for further tasks. Note that
     * this limit is counted per handle. -1 means that the number of pending calls is unlimited.
     *
     * @param maxPendingCalls The maximum number of pending calls for this actor.
     * @return self
     */
    public Builder setMaxPendingCalls(int maxPendingCalls) {
      if (maxPendingCalls < -1 || maxPendingCalls == 0) {
        throw new IllegalArgumentException(
            "maxPendingCalls must be greater than 0, or -1 to disable.");
      }

      this.maxPendingCalls = maxPendingCalls;
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
          name,
          lifetime,
          resources,
          maxRestarts,
          jvmOptions,
          maxConcurrency,
          group,
          bundleIndex,
          concurrencyGroups,
          maxPendingCalls);
    }

    /** Set the concurrency groups for this actor. */
    public Builder setConcurrencyGroups(List<ConcurrencyGroup> concurrencyGroups) {
      this.concurrencyGroups = concurrencyGroups;
      return this;
    }
  }
}
