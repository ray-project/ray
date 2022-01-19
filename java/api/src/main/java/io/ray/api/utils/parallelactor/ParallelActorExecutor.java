package io.ray.api.utils.parallelactor;

import io.ray.api.function.RayFuncR;

import java.util.List;

public class ParallelActorExecutor {

  private ParallelStrategy strategy;
//
  private int parallelNum;

  public ParallelActorExecutor(ParallelStrategy strategy, int parallelNum) {
    this.strategy = strategy;
    this.parallelNum = parallelNum;
  }

}
