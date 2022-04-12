package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.parallelactor.ParallelActor;
import io.ray.api.parallelactor.ParallelActorExecutor;
import io.ray.api.parallelactor.ParallelInstance;
import io.ray.api.parallelactor.ParallelStrategyInterface;

public class ParallelActorImpl<A> implements ParallelActor<A> {

  private ParallelStrategyInterface strategy;
  private ActorHandle<? extends ParallelActorExecutor> parallelExecutorHandle = null;

  public ParallelActorImpl(
      ParallelStrategyInterface strategy, ActorHandle<? extends ParallelActorExecutor> handle) {
    this.strategy = strategy;
    parallelExecutorHandle = handle;
  }

  @Override
  public ParallelInstance<A> getInstance(int index) {
    // TODO(qwang): Not new this object every time.
    return new ParallelInstance(this, index);
  }

  @Override
  public ActorHandle<? extends ParallelActorExecutor> getExecutor() {
    return parallelExecutorHandle;
  }

  @Override
  public ParallelStrategyInterface getStrategy() {
    return strategy;
  }
}
