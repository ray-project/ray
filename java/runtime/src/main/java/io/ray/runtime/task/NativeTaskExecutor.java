package io.ray.runtime.task;

import io.ray.api.id.UniqueId;
import io.ray.runtime.AbstractRayRuntime;

/**
 * Task executor for cluster mode.
 *
 * <p>In cluster mode, each worker process serves exactly one actor. The C++ core worker
 * runs {@code ACTOR_CREATION_TASK} on the main event-loop thread and dispatches subsequent
 * {@code ACTOR_TASK}s to a BoundedExecutor thread pool — potentially a different thread.
 * Therefore the actor context is stored as a shared volatile field so that it is visible
 * across all threads within the same worker process.
 */
public class NativeTaskExecutor extends TaskExecutor<NativeTaskExecutor.NativeActorContext> {

  static class NativeActorContext extends TaskExecutor.ActorContext {}

  /** Shared across threads; safe because one worker process serves exactly one actor. */
  private volatile NativeActorContext actorContext;

  public NativeTaskExecutor(AbstractRayRuntime runtime) {
    super(runtime);
  }

  @Override
  protected NativeActorContext createActorContext() {
    return new NativeActorContext();
  }

  @Override
  protected NativeActorContext getActorContext() {
    return actorContext;
  }

  @Override
  protected void setActorContext(UniqueId workerId, NativeActorContext actorContext) {
    this.actorContext = actorContext;
  }
}
