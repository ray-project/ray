package io.ray.serve.model;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.ray.runtime.serializer.MessagePackSerializer;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Configuration options for a replica. */
public class ReplicaConfig implements Serializable {

  private static final long serialVersionUID = -1442657824045704226L;

  private static Gson gson = new Gson();

  private String deploymentDef;

  private Object[] initArgs;

  private Map<String, Object> rayActorOptions;

  private Map<String, Double> resource;

  private static final String[] disallowedRayActorOptions = {
    "args",
    "kwargs",
    "max_concurrency",
    "max_restarts",
    "max_task_retries",
    "name",
    "namespace",
    "lifetime",
    "placement_group",
    "placement_group_bundle_index",
    "placement_group_capture_child_tasks",
    "max_pending_calls",
    "scheduling_strategy"
  };

  @SuppressWarnings("unchecked")
  public ReplicaConfig(
      String deploymentDef, Object[] initArgs, Map<String, Object> rayActorOptions) {
    this.deploymentDef = deploymentDef;
    this.initArgs = initArgs != null ? initArgs : new Object[0];
    this.rayActorOptions = rayActorOptions != null ? rayActorOptions : new HashMap<>();
    this.resource = new HashMap<>();
    this.validate();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void validate() {

    for (String option : disallowedRayActorOptions) {
      Preconditions.checkArgument(
          !rayActorOptions.containsKey(option),
          String.format("Specifying %s in ray_actor_options is not allowed.", option));
    }

    // Ray defaults to zero CPUs for placement, we default to one here.
    Object numCpus = rayActorOptions.computeIfAbsent("num_cpus", key -> 1.0);
    Preconditions.checkArgument(
        numCpus instanceof Double, "num_cpus in rayActorOptions must be a double.");
    Preconditions.checkArgument(
        ((Double) numCpus) >= 0, "num_cpus in rayActorOptions must be >= 0.");
    resource.put("CPU", (Double) numCpus);

    Object numGpus = rayActorOptions.computeIfAbsent("num_gpus", key -> 0.0);
    Preconditions.checkArgument(
        numGpus instanceof Double, "num_gpus in rayActorOptions must be a double.");
    Preconditions.checkArgument(
        ((Double) numGpus) >= 0, "num_gpus in rayActorOptions must be >= 0.");
    resource.put("GPU", (Double) numGpus);

    Object memory = rayActorOptions.computeIfAbsent("memory", key -> 0.0);
    Preconditions.checkArgument(
        memory instanceof Double, "memory in rayActorOptions must be a double.");
    Preconditions.checkArgument(((Double) memory) >= 0, "memory in rayActorOptions must be >= 0.");
    resource.put("memory", (Double) memory);

    Object objectStoreMemory = rayActorOptions.computeIfAbsent("object_store_memory", key -> 0.0);
    Preconditions.checkArgument(
        objectStoreMemory instanceof Double,
        "object_store_memory in rayActorOptions must be a double.");
    Preconditions.checkArgument(
        ((Double) objectStoreMemory) >= 0, "object_store_memory in rayActorOptions must be >= 0.");
    resource.put("object_store_memory", (Double) objectStoreMemory);

    Object customResources =
        rayActorOptions.computeIfAbsent("resources", key -> Collections.emptyMap());
    Preconditions.checkArgument(
        customResources instanceof Map, "resources in rayActorOptions must be a map.");
    resource.putAll((Map) customResources);
  }

  public byte[] toProtoBytes() {
    io.ray.serve.generated.ReplicaConfig.Builder builder =
        io.ray.serve.generated.ReplicaConfig.newBuilder();
    if (StringUtils.isNotBlank(deploymentDef)) {
      builder.setSerializedDeploymentDef(
          ByteString.copyFromUtf8(
              deploymentDef)); // TODO controller distinguish java and deserialize it.
    }
    if (initArgs != null && initArgs.length > 0) {
      builder.setInitArgs(
          ByteString.copyFrom(
              MessagePackSerializer.encode(initArgs)
                  .getKey())); // TODO controller distinguish java and deserialize it.
    }
    if (rayActorOptions != null && !rayActorOptions.isEmpty()) {
      builder.setRayActorOptions(gson.toJson(rayActorOptions));
    }

    return builder.build().toByteArray();
  }

  public String getDeploymentDef() {
    return deploymentDef;
  }

  public void setDeploymentDef(String deploymentDef) {
    this.deploymentDef = deploymentDef;
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
