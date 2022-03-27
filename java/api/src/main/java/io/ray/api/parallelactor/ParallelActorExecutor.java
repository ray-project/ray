package io.ray.api.parallelactor;

public class ParallelActorExecutor {

  private ParallelStrategy strategy;

  private int parallelNum;

  private int nextInstanceIndex = 0;

  public ParallelActorExecutor(ParallelStrategy strategy, int parallelNum) {
    this.strategy = strategy;
    this.parallelNum = parallelNum;
  }

  public int getNextIndex() {
    if (strategy == ParallelStrategy.ALWAYS_FIRST) {
      return 0;
    }

    if (strategy == ParallelStrategy.ROUND_ROBIN) {
      return (nextInstanceIndex++ + 1) % parallelNum;
    }

    return 0;
  }
}
