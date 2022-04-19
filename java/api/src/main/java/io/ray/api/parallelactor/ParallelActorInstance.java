package io.ray.api.parallelactor;

public class ParallelActorInstance<A> implements ActorCall<A> {

  private ParallelActorHandle<A> parallelActorHandle;

  private int currentIndex;

  public ParallelActorInstance(ParallelActorHandle<A> parallelActorHandle, int index) {
    this.parallelActorHandle = parallelActorHandle;
    this.currentIndex = index;
  }

  ParallelActorHandle<A> getActor() {
    return parallelActorHandle;
  }

  int getIndex() {
    return currentIndex;
  }
}
