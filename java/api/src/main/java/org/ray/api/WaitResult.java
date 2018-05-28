package org.ray.api;


/**
 * The result of Ray.wait() distinguish the ready ones and the remain ones
 */
public class WaitResult<T> {

  private final RayList<T> readyOnes;
  private final RayList<T> remainOnes;

  public WaitResult(RayList<T> readyOnes, RayList<T> remainOnes) {
    this.readyOnes = readyOnes;
    this.remainOnes = remainOnes;
  }

  public RayList<T> getReadyOnes() {
    return readyOnes;
  }

  public RayList<T> getRemainOnes() {
    return remainOnes;
  }

}
