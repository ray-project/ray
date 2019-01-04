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

  /**
   * Create a handle with different ID. If `random` is false,
   * new handle's ID will be generated deterministically.
   * Otherwise, new handle's ID will be random.
   */
  RayActor<T> fork(boolean random);
}
