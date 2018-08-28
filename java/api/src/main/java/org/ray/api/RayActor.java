package org.ray.api;

/**
 * A handle to an actor.
 * @param <T> the type of the concrete actor class.
 */
public interface RayActor<T> {

  UniqueID getId();

  UniqueID getHandleId();
}
