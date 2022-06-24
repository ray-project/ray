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

  /**
   * Fetch the object from the object store, this method will block until the object is locally
   * available.
   *
   * @param timeoutMs The maximum amount of time in miliseconds to wait before returning.
   * @throws RayTimeoutException If it's timeout to get the object.
   */
  T get(long timeoutMs);
}
