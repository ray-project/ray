package org.ray.runtime.task;

import org.ray.api.id.ActorId;
import org.ray.runtime.RayRuntimeInternal;
import org.ray.runtime.task.TaskExecutor.ActorContext;

/**
 * Task executor for local mode.
 */
public class LocalModeTaskExecutor extends TaskExecutor<ActorContext> {

  public LocalModeTaskExecutor(RayRuntimeInternal runtime) {
    super(runtime);
  }

  @Override
  protected ActorContext createActorContext() {
    return new ActorContext();
  }

  @Override
  protected void maybeSaveCheckpoint(Object actor, ActorId actorId) {
  }

  @Override
  protected void maybeLoadCheckpoint(Object actor, ActorId actorId) {
  }
}
