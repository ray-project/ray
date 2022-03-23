package io.ray.api.parallelactor;

public class ParallelActorExecutor {

  private ParallelStrategy strategy;
//
  private int parallelNum;

  public ParallelActorExecutor(ParallelStrategy strategy, int parallelNum) {
    this.strategy = strategy;
    this.parallelNum = parallelNum;
  }

}
