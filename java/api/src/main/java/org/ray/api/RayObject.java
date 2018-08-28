package org.ray.api;

import java.io.Serializable;

/**
 * Represents an object in the object store.
 * @param <T> the object type.
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
  UniqueID getId();

}

