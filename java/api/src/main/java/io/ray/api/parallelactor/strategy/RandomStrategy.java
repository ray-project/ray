package io.ray.api.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategyInterface;
import java.io.Serializable;
import java.util.Random;

public class RandomStrategy implements ParallelStrategyInterface, Serializable {

  private static Random random = new Random();

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
    return random.nextInt(parallelNum);
  }

  @Override
  public void reset() {}
}
