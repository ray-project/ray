package org.ray.api;


import java.util.List;

/**
 * The result of Ray.wait() distinguish the ready ones and the remain ones
 */
public class WaitResult<T> {

  private final List<RayObject<T>> readyOnes;
  private final List<RayObject<T>> remainOnes;

  public WaitResult(List<RayObject<T>> readyOnes, List<RayObject<T>> remainOnes) {
    this.readyOnes = readyOnes;
    this.remainOnes = remainOnes;
  }

  public List<RayObject<T>> getReadyOnes() {
    return readyOnes;
  }

  public List<RayObject<T>> getRemainOnes() {
    return remainOnes;
  }

}
