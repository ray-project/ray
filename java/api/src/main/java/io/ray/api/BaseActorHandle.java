package io.ray.api;

import io.ray.api.id.ActorId;

/**
 * A handle to an actor.
 *
 * <p>A handle can be used to invoke a remote actor method.
 */
public interface BaseActorHandle {

  /** Returns the id of this actor. */
  ActorId getId();

  /**
   * Kill the actor immediately. This will cause any outstanding tasks submitted to the actor to
   * fail and the actor to exit in the same way as if it crashed. The killed actor will not be
   * restarted anymore.
   */
  default void kill() {
    Ray.internal().killActor(this, true);
  }

  /**
   * Kill the actor immediately. This will cause any outstanding tasks submitted to the actor to
   * fail and the actor to exit in the same way as if it crashed.
   *
   * @param noRestart If set to true, the killed actor will not be restarted anymore.
   */
  default void kill(boolean noRestart) {
    Ray.internal().killActor(this, noRestart);
  }
}
