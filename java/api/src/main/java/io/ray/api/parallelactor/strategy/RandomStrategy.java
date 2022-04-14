package io.ray.api.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategyInterface;

public class RandomStrategy implements ParallelStrategyInterface {

  private int parallelNum = 1;

  public RandomStrategy(int parallelNum) {
    this.parallelNum = parallelNum;
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
