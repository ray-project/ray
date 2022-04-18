package io.ray.api.parallelactor;

public class ParallelInstance<A> implements ParallelActorCall<A> {

  private ParallelActorHandle<A> parallelActorHandle;

  private int currentIndex;

  public ParallelInstance(ParallelActorHandle<A> parallelActorHandle, int index) {
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
