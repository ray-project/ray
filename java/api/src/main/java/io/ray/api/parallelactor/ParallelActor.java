package io.ray.api.parallelactor;

import io.ray.api.ActorHandle;

public class ParallelActor<A> implements ParallelActorCall<A> {

  private ParallelStrategyInterface strategy;
  private ActorHandle<? extends ParallelActorExecutor> parallelExecutorHandle = null;

  public ParallelActor(
      ParallelStrategyInterface strategy, ActorHandle<? extends ParallelActorExecutor> handle) {
    this.strategy = strategy;
    parallelExecutorHandle = handle;
  }

  public ParallelInstance<A> getInstance(int index) {
    // TODO(qwang): Not new this object every time.
    return new ParallelInstance(this, index);
  }

  public ActorHandle<? extends ParallelActorExecutor> getExecutor() {
    return parallelExecutorHandle;
  }

  public ParallelStrategyInterface getStrategy() {
    return strategy;
  }
}
