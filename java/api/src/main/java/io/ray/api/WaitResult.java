package io.ray.api;

import java.util.List;

/**
 * Represents the result of a Ray.wait call. It contains 2 lists, one containing the locally
 * available objects, one containing the rest.
 */
public final class WaitResult<T> {

  private final List<ObjectRef<T>> ready;
  private final List<ObjectRef<T>> unready;

  public WaitResult(List<ObjectRef<T>> ready, List<ObjectRef<T>> unready) {
    this.ready = ready;
    this.unready = unready;
  }

  /** Get the list of ready objects. */
  public List<ObjectRef<T>> getReady() {
    return ready;
  }

  /** Get the list of unready objects. */
  public List<ObjectRef<T>> getUnready() {
    return unready;
  }
}
