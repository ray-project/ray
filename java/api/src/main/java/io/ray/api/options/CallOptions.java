package io.ray.api.options;

import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.runtimeenv.RuntimeEnv;
import java.util.HashMap;
import java.util.Map;

/** The options for RayCall. */
public class CallOptions extends BaseTaskOptions {

  public final String name;
  public final PlacementGroup group;
  public final int bundleIndex;
  public final String concurrencyGroupName;
  private final String serializedRuntimeEnvInfo;

  private CallOptions(
      String name,
      Map<String, Double> resources,
      PlacementGroup group,
      int bundleIndex,
      String concurrencyGroupName,
      RuntimeEnv runtimeEnv) {
    super(resources);
    this.name = name;
    this.group = group;
    this.bundleIndex = bundleIndex;
    this.concurrencyGroupName = concurrencyGroupName;
    this.serializedRuntimeEnvInfo = runtimeEnv == null ? "" : runtimeEnv.toJsonBytes();
  }

  /** This inner class for building CallOptions. */
  public static class Builder {

    private String name;
    private Map<String, Double> resources = new HashMap<>();
    private PlacementGroup group;
    private int bundleIndex;
    private String concurrencyGroupName = "";
    private RuntimeEnv runtimeEnv = null;

    /**
     * Set a name for this task.
     *
     * @param name task name
     * @return self
     */
    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    /**
     * Set a custom resource requirement for resource {@code name}. This method can be called
     * multiple times. If the same resource is set multiple times, the latest quantity will be used.
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
     * Set custom requirements for multiple resources. This method can be called multiple times. If
     * the same resource is set multiple times, the latest quantity will be used.
     *
     * @param resources requirements for multiple resources.
     * @return self
     */
    public Builder setResources(Map<String, Double> resources) {
      this.resources.putAll(resources);
      return this;
    }

    /**
     * Set the placement group to place this actor in.
     *
     * @param group The placement group of the actor.
     * @param bundleIndex The index of the bundle to place this task in.
     * @return self
     */
    public Builder setPlacementGroup(PlacementGroup group, int bundleIndex) {
      this.group = group;
      this.bundleIndex = bundleIndex;
      return this;
    }

    public Builder setConcurrencyGroupName(String concurrencyGroupName) {
      this.concurrencyGroupName = concurrencyGroupName;
      return this;
    }

    public Builder setRuntimeEnv(RuntimeEnv runtimeEnv) {
      this.runtimeEnv = runtimeEnv;
      return this;
    }

    public CallOptions build() {
      return new CallOptions(name, resources, group, bundleIndex, concurrencyGroupName, runtimeEnv);
    }
  }
}
