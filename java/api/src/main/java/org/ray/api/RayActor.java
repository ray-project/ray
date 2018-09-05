package org.ray.api;

import org.ray.api.id.UniqueId;

/**
 * A handle to an actor.
 * @param <T> The type of the concrete actor class.
 */
public interface RayActor<T> {

  /**
   * @return The id of this actor.
   */
  UniqueId getId();

  /**
   * @return The id of this actor handle.
   */
  UniqueId getHandleId();
}
