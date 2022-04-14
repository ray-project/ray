package io.ray.api.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategyInterface;
import java.io.Serializable;
import java.util.Random;

public class RandomStrategy implements ParallelStrategyInterface, Serializable {

  private static final long serialVersionUID = 4760632395229286424L;

  private static final Random RANDOM = new Random();

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
    return RANDOM.nextInt(parallelNum);
  }

  @Override
  public void reset() {}
}
