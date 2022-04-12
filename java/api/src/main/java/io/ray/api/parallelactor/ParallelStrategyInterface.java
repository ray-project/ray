package io.ray.api.parallelactor;

public interface ParallelStrategyInterface {

  ParallelStrategy getStrategyEnum();

  int getParallelNum();

  int getNextIndex();
}
