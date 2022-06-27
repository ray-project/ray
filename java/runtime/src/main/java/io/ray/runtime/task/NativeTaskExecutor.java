package io.ray.runtime.task;

import io.ray.api.id.UniqueId;
import io.ray.runtime.AbstractRayRuntime;

/** Task executor for cluster mode. */
public class NativeTaskExecutor extends TaskExecutor<NativeTaskExecutor.NativeActorContext> {

  static class NativeActorContext extends TaskExecutor.ActorContext {}

  public NativeTaskExecutor(AbstractRayRuntime runtime) {
    super(runtime);
  }

  @Override
  protected NativeActorContext createActorContext() {
    return new NativeActorContext();
  }

  public void onWorkerShutdown(byte[] workerIdBytes) {
    // This is to make sure no memory leak when `Ray.exitActor()` is called.
    removeActorContext(new UniqueId(workerIdBytes));
  }
}
