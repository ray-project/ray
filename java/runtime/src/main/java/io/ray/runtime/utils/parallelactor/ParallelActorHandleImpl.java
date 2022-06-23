package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.parallelactor.ParallelActorHandle;
import io.ray.api.parallelactor.ParallelActorInstance;
import java.io.Serializable;

public class ParallelActorHandleImpl<A> implements ParallelActorHandle<A>, Serializable {

  private int parallelism = 1;

  private ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle = null;

  // An empty ctor for FST serializing need.
  public ParallelActorHandleImpl() {}

  public ParallelActorHandleImpl(int parallelism, ActorHandle<ParallelActorExecutorImpl> handle) {
    this.parallelism = parallelism;
    parallelExecutorHandle = handle;
  }

  @Override
  public ParallelActorInstance<A> getInstance(int instanceId) {
    return new ParallelActorInstance<A>(this, instanceId);
  }

  public ActorHandle<? extends ParallelActorExecutorImpl> getExecutor() {
    return parallelExecutorHandle;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public ActorHandle<?> getHandle() {
    return parallelExecutorHandle;
  }
}
