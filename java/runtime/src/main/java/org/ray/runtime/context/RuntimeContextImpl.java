package org.ray.runtime.context;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.api.id.ActorId;
import org.ray.api.id.JobId;
import org.ray.api.runtimecontext.NodeInfo;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.generated.Common.TaskType;

public class RuntimeContextImpl implements RuntimeContext {

  private AbstractRayRuntime runtime;

  public RuntimeContextImpl(AbstractRayRuntime runtime) {
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
  public boolean wasCurrentActorReconstructed() {
    TaskType currentTaskType = runtime.getWorkerContext().getCurrentTaskType();
    Preconditions.checkState(currentTaskType == TaskType.ACTOR_CREATION_TASK,
        "This method can only be called from an actor creation task.");
    if (isSingleProcess()) {
      return false;
    }

    return runtime.getGcsClient().actorExists(getCurrentActorId());
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
