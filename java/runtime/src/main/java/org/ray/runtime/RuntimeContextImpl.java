package org.ray.runtime;

import com.google.common.base.Preconditions;
import org.ray.api.RuntimeContext;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.config.WorkerMode;
import org.ray.runtime.task.TaskSpec;

public class RuntimeContextImpl implements RuntimeContext {

  private AbstractRayRuntime runtime;

  public RuntimeContextImpl(AbstractRayRuntime runtime) {
    this.runtime = runtime;
  }

  @Override
  public UniqueId getCurrentDriverId() {
    return runtime.getWorkerContext().getCurrentDriverId();
  }

  @Override
  public UniqueId getCurrentActorId() {
    Preconditions.checkState(runtime.rayConfig.workerMode == WorkerMode.WORKER);
    return runtime.getWorker().getCurrentActorId();
  }

  @Override
  public boolean wasCurrentActorReconstructed() {
    TaskSpec currentTask = runtime.getWorkerContext().getCurrentTask();
    Preconditions.checkState(currentTask != null && currentTask.isActorCreationTask(),
        "This method can only be called from an actor creation task.");
    if (isSingleProcess()) {
      return false;
    }

    return ((RayNativeRuntime) runtime).actorExistsInGcs(getCurrentActorId());
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

}
