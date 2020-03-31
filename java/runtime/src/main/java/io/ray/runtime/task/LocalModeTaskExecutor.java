package io.ray.runtime.task;

import io.ray.api.id.ActorId;
import io.ray.runtime.AbstractRayRuntime;

/**
 * Task executor for local mode.
 */
public class LocalModeTaskExecutor extends TaskExecutor {

  public LocalModeTaskExecutor(AbstractRayRuntime runtime) {
    super(runtime);
  }

  @Override
  protected void maybeSaveCheckpoint(Object actor, ActorId actorId) {
  }

  @Override
  protected void maybeLoadCheckpoint(Object actor, ActorId actorId) {
  }
}
