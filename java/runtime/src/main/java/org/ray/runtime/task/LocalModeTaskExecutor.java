package org.ray.runtime.task;

import org.ray.api.id.ActorId;
import org.ray.runtime.AbstractRayRuntime;

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
