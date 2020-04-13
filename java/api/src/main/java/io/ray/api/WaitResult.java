package io.ray.api;

import java.util.List;

/**
 * Represents the result of a Ray.wait call. It contains 2 lists,
 * one containing the locally available objects, one containing the rest.
 */
public final class WaitResult<T> {

  private final List<RayObject<T>> ready;
  private final List<RayObject<T>> unready;

  public WaitResult(List<RayObject<T>> ready, List<RayObject<T>> unready) {
    this.ready = ready;
    this.unready = unready;
  }

  /**
   * Get the list of ready objects.
   */
  public List<RayObject<T>> getReady() {
    return ready;
  }

  /**
   * Get the list of unready objects.
   */
  public List<RayObject<T>> getUnready() {
    return unready;
  }

}
