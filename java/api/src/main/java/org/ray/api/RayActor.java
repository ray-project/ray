package org.ray.api;

/**
 * A handle to an actor.
 * @param <T> The type of the concrete actor class.
 */
public interface RayActor<T> {

  /**
   * @return The id of this actor.
   */
  UniqueID getId();

  /**
   * @return The id of this actor handle.
   */
  UniqueID getHandleId();
}
