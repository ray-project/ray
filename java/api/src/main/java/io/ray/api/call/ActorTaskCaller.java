package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ActorTaskCaller<R> {
  private final ActorHandle actor;
  private final RayFuncR<R> func;
  private final Object[] args;

  public ActorTaskCaller(ActorHandle actor, RayFuncR<R> func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  @SuppressWarnings("unchecked")
  public ObjectRef<R> remote() {
    return Ray.internal().callActor(actor, func, args);
  }

}
