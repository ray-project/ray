package org.ray.runtime;

import org.ray.api.RuntimeContext;
import org.ray.api.id.UniqueId;
import org.ray.runtime.config.RunMode;

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
    return runtime.getCurrentActorId();
  }

  @Override
  public boolean wasCurrentActorReconstructed() {
    if (RunMode.SINGLE_PROCESS == runtime.getRayConfig().runMode) {
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

}
