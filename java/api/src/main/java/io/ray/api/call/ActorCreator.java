package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFunc;

public class ActorCreator<A> extends BaseActorCreator {
  private final RayFunc<A> func;
  private final Object[] args;

  public ActorCreator(RayFunc<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  public ActorHandle<A> remote() {
    return Ray.internal().createActor(func, args, createActorCreationOptions());
  }

}
