package io.ray.api.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.call.ActorTaskCaller;

public class ParallelActor<A> implements ParallelActorCall<A> {

  private ParallelStrategy strategy;
  private int parallelNum;
  private int lastIndex = -1;
  private ActorHandle<? extends ParallelActorExecutor> parallelExecutorHandle = null;

  public ParallelActor(ParallelStrategy strategy, int parallelNum, ActorHandle<? extends ParallelActorExecutor> handle) {
    this.strategy = strategy;
    this.parallelNum = parallelNum;
    parallelExecutorHandle = handle;
  }

  public ParallelInstance<A> getParallel(int index) {
    return null;
  }

  public ActorHandle<? extends ParallelActorExecutor> getParallelExecutorHandle() {
    return parallelExecutorHandle;
  }

  public ParallelStrategy getStrategy() {
    return strategy;
  }

  public int getParallelNum() {
    return parallelNum;
  }

  public int getNextIndex() {
    if (strategy == ParallelStrategy.ALWAYS_FIRST) {
      return 0;
    }

    if (strategy == ParallelStrategy.ROUND_ROBIN) {
      return (lastIndex++ + 1) % parallelNum;
    }

    return 0;
  }
}
