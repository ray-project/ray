package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;
import io.ray.api.options.CallOptions;

/**
 * A helper to call java actor method.
 *
 * @param <R> The type of the java actor method return value
 */
public class ActorTaskCaller<R> {
  private final ActorHandle actor;
  private final RayFuncR<R> func;
  private final Object[] args;
  private CallOptions.Builder builder = new CallOptions.Builder();

  public ActorTaskCaller(ActorHandle actor, RayFuncR<R> func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  public ActorTaskCaller<R> setConcurrencyGroup(String name) {
    builder.setConcurrencyGroupName(name);
    return self();
  }

  private ActorTaskCaller<R> self() {
    return this;
  }

  /**
   * Execute an java actor method remotely and return an object reference to the result object in
   * the object store.
   *
   * @return an object reference to an object in the object store.
   */
  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, func, args, builder.build());
  }
}
