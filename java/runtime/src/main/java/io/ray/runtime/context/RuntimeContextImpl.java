package io.ray.runtime.context;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.TaskId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.util.ResourceUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RuntimeContextImpl implements RuntimeContext {

  private RayRuntimeInternal runtime;

  public RuntimeContextImpl(RayRuntimeInternal runtime) {
    this.runtime = runtime;
  }

  @Override
  public JobId getCurrentJobId() {
    return runtime.getWorkerContext().getCurrentJobId();
  }

  @Override
  public ActorId getCurrentActorId() {
    ActorId actorId = runtime.getWorkerContext().getCurrentActorId();
    Preconditions.checkState(
        actorId != null && !actorId.isNil(), "This method should only be called from an actor.");
    return actorId;
  }

  @Override
  public TaskId getCurrentTaskId() {
    return runtime.getWorkerContext().getCurrentTaskId();
  }

  @Override
  public boolean wasCurrentActorRestarted() {
    if (isSingleProcess()) {
      return false;
    }
    return runtime.getGcsClient().wasCurrentActorRestarted(getCurrentActorId());
  }

  @Override
  public boolean isSingleProcess() {
    return RunMode.SINGLE_PROCESS == runtime.getRayConfig().runMode;
  }

  @Override
  public List<NodeInfo> getAllNodeInfo() {
    return runtime.getGcsClient().getAllNodeInfo();
  }

  @Override
  public <T extends BaseActorHandle> T getCurrentActorHandle() {
    return runtime.getActorHandle(getCurrentActorId());
  }

  @Override
  public List<Long> getGpuIds() {
    Map<String, List<Long>> resourceIds = runtime.getAvailableResourceIds();
    Set<Long> assignedIds = new HashSet<>();
    for (Map.Entry<String, List<Long>> entry : resourceIds.entrySet()) {
      if (entry.getKey().equals("GPU") || entry.getKey().startsWith("GPU_group_")) {
        assignedIds.addAll(entry.getValue());
      }
    }
    List<Long> gpuIds;
    List<String> gpuOnThisNode = ResourceUtil.getCudaVisibleDevices();
    if (gpuOnThisNode != null) {
      gpuIds = new ArrayList<>();
      for (Long id : assignedIds) {
        gpuIds.add(Long.valueOf(gpuOnThisNode.get(id.intValue())));
      }
    } else {
      gpuIds = new ArrayList<>(assignedIds);
    }
    return gpuIds;
  }
}
