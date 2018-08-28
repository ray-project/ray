package org.ray.api;

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
  UniqueID getId();

}

