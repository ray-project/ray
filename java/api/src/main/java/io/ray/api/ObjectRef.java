package io.ray.api;

/**
 * Represents a reference to an object in the object store.
 *
 * @param <T> The object type.
 */
public interface ObjectRef<T> {

  /**
   * Fetch the object from the object store, this method will block until the object is locally
   * available.
   */
  T get();
}
