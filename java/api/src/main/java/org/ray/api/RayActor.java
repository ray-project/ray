package org.ray.api;

import org.ray.api.id.ActorId;

/**
 * A handle to an actor. <p>
 *
 * A handle can be used to invoke a remote actor method.
 */
public interface RayActor {

  /**
   * @return The id of this actor.
   */
  ActorId getId();

}
