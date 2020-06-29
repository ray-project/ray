package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;

/**
 * A helper to call java actor method which doesn't have a return value.
 */
public class VoidActorTaskCaller {
  private final ActorHandle actor;
  private final RayFuncVoid func;
  private final Object[] args;

  public VoidActorTaskCaller(ActorHandle actor, RayFuncVoid func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  /**
   * Execute a function remotely.
   */
  public void remote() {
    Ray.internal().callActor(actor, func, args);
  }

}
