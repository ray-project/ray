package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.parallelactor.ParallelActor;
import io.ray.api.parallelactor.ParallelInstance;
import io.ray.api.parallelactor.ParallelStrategyInterface;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class ParallelActorImpl<A> implements ParallelActor<A>, Externalizable {

  private ParallelStrategyInterface strategy;

  private ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle = null;

  // An empty ctor for FST serializing need.
  public ParallelActorImpl() {}

  public ParallelActorImpl(
      ParallelStrategyInterface strategy, ActorHandle<ParallelActorExecutorImpl> handle) {
    this.strategy = strategy;
    parallelExecutorHandle = handle;
  }

  @Override
  public ParallelInstance<A> getInstance(int index) {
    return new ParallelInstance<A>(this, index);
  }

  public ActorHandle<? extends ParallelActorExecutorImpl> getExecutor() {
    return parallelExecutorHandle;
  }

  @Override
  public ParallelStrategyInterface getStrategy() {
    return strategy;
  }

  @Override
  public ActorHandle<?> getHandle() {
    return parallelExecutorHandle;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(parallelExecutorHandle);
    out.writeObject(strategy);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    this.parallelExecutorHandle = (ActorHandle<ParallelActorExecutorImpl>) in.readObject();
    this.strategy = (ParallelStrategyInterface) in.readObject();
    // Note that this is necessary so that the strategy could be passed as argument in
    // remote call. A new worker shouldn't resume from the old one.
    this.strategy.reset();
  }
}
