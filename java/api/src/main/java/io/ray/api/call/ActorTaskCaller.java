package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

/**
 * A helper to call java actor method.
 *
 * @param <R> The type of the java actor method return value
 */
public class ActorTaskCaller<R> {
  private final ActorHandle actor;
  private final RayFuncR<R> func;
  private final Object[] args;

  public ActorTaskCaller(ActorHandle actor, RayFuncR<R> func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  /**
   * Execute an java actor method remotely and return an object reference to the result object in
   * the object store.
   *
   * <p>Returns an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, func, args);
  }
}
