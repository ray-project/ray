package io.ray.runtime.utils.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategy;
import io.ray.api.parallelactor.ParallelStrategyInterface;

public class AlwaysFirstStrategy implements ParallelStrategyInterface {

  private int parallelNum = 1;

  public AlwaysFirstStrategy(int parallelNum) {
    this.parallelNum = parallelNum;
  }

  @Override
  public ParallelStrategy getStrategyEnum() {
    return ParallelStrategy.ALWAYS_FIRST;
  }

  @Override
  public int getParallelNum() {
    return parallelNum;
  }

  @Override
  public int getNextIndex() {
    return 0;
  }
}
