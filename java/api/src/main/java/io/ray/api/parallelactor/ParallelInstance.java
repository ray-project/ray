package io.ray.api.parallelactor;

public class ParallelInstance<A> implements ParallelActorCall<A> {

  private ParallelActor<A> parallelActor;

  private int currentIndex;

  public ParallelInstance(ParallelActor<A> parallelActor, int index) {
    this.parallelActor = parallelActor;
    this.currentIndex = index;
  }

  ParallelActor<A> getActor() {
    return parallelActor;
  }

  int getIndex() {
    return currentIndex;
  }
}
