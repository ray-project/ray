package io.ray.runtime.task;

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
}
