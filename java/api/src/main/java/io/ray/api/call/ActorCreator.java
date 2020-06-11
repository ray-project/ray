package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

/**
 * A helper to create java actor.
 *
 * @param <A> The type of the concrete actor class.
 */
public class ActorCreator<A> extends BaseActorCreator<ActorCreator<A>> {
  private final RayFuncR<A> func;
  private final Object[] args;

  public ActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /**
   * Create a java actor remotely and return a handle to the created java actor
   *
   * @return a handle to the created java actor
   */
  public ActorHandle<A> remote() {
    return Ray.internal().createActor(func, args, buildOptions());
  }

}
