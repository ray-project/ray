package io.ray.api.utils.parallelactor;

import io.ray.api.function.RayFuncR;

import java.util.List;

public abstract class ParallelActorExecutor {

  private ParallelStrategy strategy;
//
  private int parallelNum;

  public ParallelActorExecutor(ParallelStrategy strategy, int parallelNum) {
    /// 创建instances

    this.strategy = strategy;
    this.parallelNum = parallelNum;
  }

//  public Object execute(int instanceIndex, RayFuncR func,  Object[] args) {
//    if (instanceIndex < 0 || instanceIndex >= parallelNum) {
//      throw new RuntimeException("Index of parallel instance is out of range.");
//    }
//
//
//    return 100;
//  }

  protected abstract Object internalExecute(int index);


}
