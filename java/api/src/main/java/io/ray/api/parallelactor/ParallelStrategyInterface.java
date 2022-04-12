package io.ray.api.parallelactor;

// TODO(qwang): Remove this public
public interface ParallelStrategyInterface {

  ParallelStrategy getStrategyEnum();

  int getParallelNum();

  int getNextIndex();
}
