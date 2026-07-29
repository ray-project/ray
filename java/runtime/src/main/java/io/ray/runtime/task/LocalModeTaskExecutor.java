package io.ray.runtime.task;

import io.ray.api.id.UniqueId;
import io.ray.runtime.AbstractRayRuntime;

/**
 * Task executor for local mode.
 *
 * <p>In local mode, multiple actors may share the same {@code TaskExecutor} instance but run on
 * different threads (each actor is dispatched to its own executor service thread). A {@code
 * ThreadLocal} is used to isolate each actor's context per thread, preventing cross-actor
 * interference.
 */
public class LocalModeTaskExecutor extends TaskExecutor<LocalModeTaskExecutor.LocalActorContext> {

  static class LocalActorContext extends TaskExecutor.ActorContext {

    /** The worker ID of the actor. */
    private final UniqueId workerId;

    public LocalActorContext(UniqueId workerId) {
      this.workerId = workerId;
    }

    public UniqueId getWorkerId() {
      return workerId;
    }
  }

  /** Thread-local storage: each actor thread holds its own context. */
  private final ThreadLocal<LocalActorContext> actorContext = new ThreadLocal<>();

  public LocalModeTaskExecutor(AbstractRayRuntime runtime) {
    super(runtime);
  }

  @Override
  protected LocalActorContext createActorContext() {
    return new LocalActorContext(runtime.getWorkerContext().getCurrentWorkerId());
  }

  @Override
  protected LocalActorContext getActorContext() {
    return actorContext.get();
  }

  @Override
  protected void setActorContext(UniqueId workerId, LocalActorContext actorContext) {
    if (actorContext == null) {
      this.actorContext.remove();
    } else {
      this.actorContext.set(actorContext);
    }
  }
}
