package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ActorCreator<A> extends ActorCreatorBase {
  private final RayFuncR<A> func;
  private final Object[] args;

  public ActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  public ActorHandle<A> remote() {
    return Ray.internal().createActor(func, args, createActorCreationOptions());
  }

}
