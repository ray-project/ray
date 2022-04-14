package io.ray.api.parallelactor;

public interface ParallelStrategyInterface {

  int getParallelNum();

  int getNextIndex();

  void reset();
}
