package org.ray.api;


import java.util.List;

/**
 * Represents the result of a Ray.wait call. It contains 2 lists,
 * one containing the locally available objects, one containing the rest.
 */
public class WaitResult<T> {

  private final List<RayObject<T>> readyOnes;
  private final List<RayObject<T>> remainOnes;

  public WaitResult(List<RayObject<T>> readyOnes, List<RayObject<T>> remainOnes) {
    this.readyOnes = readyOnes;
    this.remainOnes = remainOnes;
  }

  /**
   * Get the list of ready objects.
   */
  public List<RayObject<T>> getReadyOnes() {
    return readyOnes;
  }

  /**
   * Get the list of unready objects.
   */
  public List<RayObject<T>> getRemainOnes() {
    return remainOnes;
  }

}
