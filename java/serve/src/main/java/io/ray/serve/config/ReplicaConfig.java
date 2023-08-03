package io.ray.serve.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.util.LogUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/** Configuration options for a replica. */
public class ReplicaConfig {

  private static Gson gson = new Gson();

  private String deploymentDef;

  private Object[] initArgs;

  private Map<String, Object> rayActorOptions;

  private Map<String, Object> resources;

  private static final Set<String> allowedRayActorOptions =
      Sets.newHashSet(
          // resource options
          "accelerator_type",
          "memory",
          "num_cpus",
          "num_gpus",
          "object_store_memory",
          "resources",
          // other options
          "runtime_env");

  public ReplicaConfig(
      String deploymentDef, Object[] initArgs, Map<String, Object> rayActorOptions) {
    this.deploymentDef = deploymentDef;
    this.initArgs = initArgs != null ? initArgs : new Object[0];
    this.rayActorOptions = rayActorOptions != null ? rayActorOptions : new HashMap<>();
    for (String option : this.rayActorOptions.keySet()) {
      Preconditions.checkArgument(
          allowedRayActorOptions.contains(option),
          LogUtil.format(
              "Specifying '{}' in ray_actor_options is not allowed. Allowed options: {}",
              option,
              allowedRayActorOptions));
    }
    // TODO validate_actor_options
    if (!this.rayActorOptions.containsKey("num_cpus")) {
      this.rayActorOptions.put("num_cpus", 1.0);
    }
    this.resources = resourcesFromRayOptions(rayActorOptions);
  }

  /**
   * Determine a task's resource requirements.
   *
   * @param rayActorOptions The map that contains resources requirements.
   * @return A map of the resource requirements for the task.
   */
  private Map<String, Object> resourcesFromRayOptions(Map<String, Object> rayActorOptions) {

    Object numCpus = rayActorOptions.get("num_cpus");
    Object numGpus = rayActorOptions.get("num_gpus");
    Object memory = rayActorOptions.get("memory");
    Object objectStoreMemory = rayActorOptions.get("object_store_memory");
    Object acceleratorType = rayActorOptions.get("accelerator_type");

    Map<String, Object> resources = new HashMap<>();
    if (numCpus != null) {
      resources.put("CPU", numCpus);
    }
    if (numGpus != null) {
      resources.put("GPU", numGpus);
    }
    if (memory != null) {
      resources.put("memory", memory);
    }
    if (objectStoreMemory != null) {
      resources.put("object_store_memory", objectStoreMemory);
    }
    if (acceleratorType != null) {
      resources.put("accelerator_type:" + acceleratorType, 0.001);
    }
    return resources;
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

  public Map<String, Object> getResources() {
    return resources;
  }

  public void setResources(Map<String, Object> resources) {
    this.resources = resources;
  }

  public byte[] toProtoBytes() {
    io.ray.serve.generated.ReplicaConfig.Builder builder =
        io.ray.serve.generated.ReplicaConfig.newBuilder();
    if (StringUtils.isNotBlank(deploymentDef)) {
      builder.setDeploymentDefName(deploymentDef);
      builder.setDeploymentDef(ByteString.copyFromUtf8(deploymentDef)); // TODO-xlang
    }
    if (initArgs != null && initArgs.length > 0) {
      builder.setInitArgs(
          ByteString.copyFrom(MessagePackSerializer.encode(initArgs).getKey())); // TODO-xlang
    }
    if (rayActorOptions != null && !rayActorOptions.isEmpty()) {
      builder.setRayActorOptions(gson.toJson(rayActorOptions));
    }
    return builder.build().toByteArray();
  }

  public static ReplicaConfig fromProto(io.ray.serve.generated.ReplicaConfig proto) {
    if (proto == null) {
      return null;
    }
    Object[] initArgs = null;
    if (0 != proto.getInitArgs().toByteArray().length) {
      initArgs = MessagePackSerializer.decode(proto.getInitArgs().toByteArray(), null);
    }
    ReplicaConfig replicaConfig =
        new ReplicaConfig(
            proto.getDeploymentDefName(),
            initArgs,
            gson.fromJson(proto.getRayActorOptions(), Map.class));
    return replicaConfig;
  }
}
