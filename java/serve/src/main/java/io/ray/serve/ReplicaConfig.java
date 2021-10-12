package io.ray.serve;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** Configuration options for a replica. */
public class ReplicaConfig implements Serializable {

  private static final long serialVersionUID = -1442657824045704226L;

  private String backendDef;

  private Object[] initArgs;

  private Map<String, Object> rayActorOptions;

  private Map<String, Double> resource;

  public ReplicaConfig(String backendDef, Object[] initArgs, Map<String, Object> rayActorOptions) {
    this.backendDef = backendDef;
    this.initArgs = initArgs;
    this.rayActorOptions = rayActorOptions;
    this.resource = new HashMap<>();
    this.validate();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void validate() {
    Preconditions.checkArgument(
        !rayActorOptions.containsKey("placement_group"),
        "Providing placement_group for backend actors is not currently supported.");

    Preconditions.checkArgument(
        !rayActorOptions.containsKey("lifetime"),
        "Specifying lifetime in init_args is not allowed.");

    Preconditions.checkArgument(
        !rayActorOptions.containsKey("name"), "Specifying name in init_args is not allowed.");

    Preconditions.checkArgument(
        !rayActorOptions.containsKey("max_restarts"),
        "Specifying max_restarts in init_args is not allowed.");

    // TODO Confirm num_cpus, num_gpus, memory is double in protobuf.
    // Ray defaults to zero CPUs for placement, we default to one here.
    Object numCpus = rayActorOptions.getOrDefault("num_cpus", 1.0);
    Preconditions.checkArgument(
        numCpus instanceof Double, "num_cpus in ray_actor_options must be a double.");
    Preconditions.checkArgument(
        ((Double) numCpus) >= 0, "num_cpus in ray_actor_options must be >= 0.");
    resource.put("CPU", (Double) numCpus);

    Object numGpus = rayActorOptions.getOrDefault("num_gpus", 0.0);
    Preconditions.checkArgument(
        numGpus instanceof Double, "num_gpus in ray_actor_options must be a double.");
    Preconditions.checkArgument(
        ((Double) numGpus) >= 0, "num_gpus in ray_actor_options must be >= 0.");
    resource.put("GPU", (Double) numGpus);

    Object memory = rayActorOptions.getOrDefault("memory", 0.0);
    Preconditions.checkArgument(
        memory instanceof Double, "memory in ray_actor_options must be a double.");
    Preconditions.checkArgument(
        ((Double) memory) >= 0, "memory in ray_actor_options must be >= 0.");
    resource.put("memory", (Double) memory);

    Object objectStoreMemory = rayActorOptions.getOrDefault("object_store_memory", 0.0);
    Preconditions.checkArgument(
        objectStoreMemory instanceof Double,
        "object_store_memory in ray_actor_options must be a double.");
    Preconditions.checkArgument(
        ((Double) objectStoreMemory) >= 0,
        "object_store_memory in ray_actor_options must be >= 0.");
    resource.put("object_store_memory", (Double) objectStoreMemory);

    Object customResources = rayActorOptions.getOrDefault("resources", new HashMap<>());
    Preconditions.checkArgument(
        customResources instanceof Map, "resources in ray_actor_options must be a map.");
    resource.putAll((Map) customResources);
  }

  public String getBackendDef() {
    return backendDef;
  }

  public void setBackendDef(String backendDef) {
    this.backendDef = backendDef;
  }

  public Object[] getInitArgs() {
    return initArgs;
  }

  public void setInitArgs(Object[] initArgs) {
    this.initArgs = initArgs;
  }

  public Map<String, Object> getRayActorOptions() {
    return rayActorOptions;
  }

  public void setRayActorOptions(Map<String, Object> rayActorOptions) {
    this.rayActorOptions = rayActorOptions;
  }

  public Map<String, Double> getResource() {
    return resource;
  }

  public void setResource(Map<String, Double> resource) {
    this.resource = resource;
  }
}
