package org.ray.api;

import org.ray.api.id.ActorId;

/**
 * A handle to an actor.
 *
 * @param <A> The type of the concrete actor class.
 */
public interface RayActor<A> extends ActorCall<A> {

  /**
   * @return The id of this actor.
   */
  ActorId getId();

  @Override
  default RayActor<A> getThis() {
    return this;
  }
}
