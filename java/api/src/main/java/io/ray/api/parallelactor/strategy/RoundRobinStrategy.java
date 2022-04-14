package io.ray.api.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategyInterface;
import java.io.*;

public class RoundRobinStrategy implements ParallelStrategyInterface, Serializable {
  // add serializing id

  private int lastIndex = -1;

  private int parallelNum = 1;

  public RoundRobinStrategy(int parallelNum) {
    this.parallelNum = parallelNum;
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
