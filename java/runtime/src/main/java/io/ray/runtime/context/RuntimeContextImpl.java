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
}
