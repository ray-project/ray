package io.ray.api;

import io.ray.api.id.ActorId;

/**
 * A handle to an actor. <p>
 *
 * A handle can be used to invoke a remote actor method.
 */
public interface BaseActor {

  /**
   * @return The id of this actor.
   */
  ActorId getId();

  /**
   * Kill the actor immediately. This will cause any outstanding tasks submitted to the actor to
   * fail and the actor to exit in the same way as if it crashed.
   *
   * @param noReconstruction If set to true, the killed actor will not be reconstructed anymore.
   */
  default void kill(boolean noReconstruction) {
    Ray.internal().killActor(this, noReconstruction);
  }
}
