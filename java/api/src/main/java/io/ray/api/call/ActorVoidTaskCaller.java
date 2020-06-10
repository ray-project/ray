package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;

public class ActorVoidTaskCaller {
  private final ActorHandle actor;
  private final RayFuncVoid func;
  private final Object[] args;

  public ActorVoidTaskCaller(ActorHandle actor, RayFuncVoid func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  public void remote() {
    Ray.internal().callActor(actor, func, args);
  }

}
