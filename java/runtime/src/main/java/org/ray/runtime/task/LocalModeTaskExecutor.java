package org.ray.runtime.task;

import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayRuntimeInternal;
import org.ray.runtime.task.LocalModeTaskExecutor.LocalActorContext;

/**
 * Task executor for local mode.
 */
public class LocalModeTaskExecutor extends TaskExecutor<LocalActorContext> {

  static class LocalActorContext extends TaskExecutor.ActorContext {

    /**
     * The worker ID of the actor.
     */
    private final UniqueId workerId;

    public LocalActorContext(UniqueId workerId) {
      this.workerId = workerId;
    }

    public UniqueId getWorkerId() {
      return workerId;
    }
  }

  public LocalModeTaskExecutor(RayRuntimeInternal runtime) {
    super(runtime);
  }

  @Override
  protected LocalActorContext createActorContext() {
    return new LocalActorContext(runtime.getWorkerContext().getCurrentWorkerId());
  }

  @Override
  protected void maybeSaveCheckpoint(Object actor, ActorId actorId) {
  }

  @Override
  protected void maybeLoadCheckpoint(Object actor, ActorId actorId) {
  }
}
