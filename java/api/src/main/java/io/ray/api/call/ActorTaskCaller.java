package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFunc;

public class ActorTaskCaller<R> {
  private final ActorHandle actor;
  private final RayFunc<R> func;
  private final Object[] args;

  public ActorTaskCaller(ActorHandle actor, RayFunc<R> func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, func, args);
  }

}
