package io.ray.runtime.utils.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategy;
import io.ray.api.parallelactor.ParallelStrategyInterface;

public class RoundRobinStrategy implements ParallelStrategyInterface {
  private int lastIndex = -1;

  private int parallelNum = 1;

  public RoundRobinStrategy(int parallelNum) {
    this.parallelNum = parallelNum;
  }

  @Override
  public ParallelStrategy getStrategyEnum() {
    return ParallelStrategy.ROUND_ROBIN;
  }

  @Override
  public int getParallelNum() {
    return parallelNum;
  }

  @Override
  public int getNextIndex() {
    // TODO: lastIndex = (lastIndex % parallelNum)
    return (++lastIndex) % parallelNum;
  }
}
