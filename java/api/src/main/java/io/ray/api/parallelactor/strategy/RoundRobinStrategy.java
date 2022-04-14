package io.ray.api.parallelactor.strategy;

import io.ray.api.parallelactor.ParallelStrategyInterface;
import java.io.*;

public class RoundRobinStrategy implements ParallelStrategyInterface, Serializable {

  private static final long serialVersionUID = -8822673941240683915L;

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
    return (++lastIndex) % parallelNum;
  }

  @Override
  public void reset() {
    lastIndex = -1;
  }
}
