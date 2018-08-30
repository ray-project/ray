package org.ray.api;

import org.ray.api.id.UniqueId;

/**
 * Represents an object in the object store.
 * @param <T> The object type.
 */
public interface RayObject<T> {

  /**
   * Fetch the object from the object store, this method will block
   * until the object is locally available.
   */
  T get();

  /**
   * Get the object id.
   */
  UniqueId getId();

}

