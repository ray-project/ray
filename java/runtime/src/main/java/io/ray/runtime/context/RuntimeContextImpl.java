package io.ray.runtime.context;

import com.google.common.base.Preconditions;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.api.runtimecontext.RuntimeContext;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.config.RunMode;
import io.ray.runtime.generated.Common.TaskType;
import java.util.List;

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
    Preconditions.checkState(actorId != null && !actorId.isNil(),
        "This method should only be called from an actor.");
    return actorId;
  }

  @Override
  public boolean wasCurrentActorRestarted() {
    TaskType currentTaskType = runtime.getWorkerContext().getCurrentTaskType();
    Preconditions.checkState(currentTaskType == TaskType.ACTOR_CREATION_TASK,
        "This method can only be called from an actor creation task.");
    if (isSingleProcess()) {
      return false;
    }

    return runtime.getGcsClient().wasCurrentActorRestarted(getCurrentActorId());
  }

  @Override
  public String getRayletSocketName() {
    return runtime.getRayConfig().rayletSocketName;
  }

  @Override
  public String getObjectStoreSocketName() {
    return runtime.getRayConfig().objectStoreSocketName;
  }

  @Override
  public boolean isSingleProcess() {
    return RunMode.SINGLE_PROCESS == runtime.getRayConfig().runMode;
  }

  @Override
  public List<NodeInfo> getAllNodeInfo() {
    return runtime.getGcsClient().getAllNodeInfo();
  }
}
