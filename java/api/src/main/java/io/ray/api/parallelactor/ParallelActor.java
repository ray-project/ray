package io.ray.api.parallelactor;

import io.ray.api.ActorHandle;

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

  public ParallelInstance<A> getInstance(int index) {
    // TODO(qwang): Not new this object every time.
    return new ParallelInstance();
  }

  public ActorHandle<? extends ParallelActorExecutor> getExecutor() {
    return parallelExecutorHandle;
  }

  public ParallelStrategy getStrategy() {
    return strategy;
  }

  public int getParallelNum() {
    return parallelNum;
  }

}
