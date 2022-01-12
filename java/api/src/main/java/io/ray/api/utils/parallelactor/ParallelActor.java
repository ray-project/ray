package io.ray.api.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.call.ActorTaskCaller;

public class ParallelActor<A> implements ParallelActorCall<A> {

  private ActorHandle<? extends ParallelActorExecutor> parallelExecutorHandle = null;

  public ParallelActor(ActorHandle<? extends ParallelActorExecutor> handle) {
    parallelExecutorHandle = handle;
  }

  public ParallelInstance<A> getParallel(int index) {
    return null;
  }

  public ActorHandle<? extends ParallelActorExecutor> getParallelExecutorHandle() {
    return parallelExecutorHandle;
  }

}
