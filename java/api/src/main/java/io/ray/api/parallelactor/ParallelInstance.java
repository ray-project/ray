package io.ray.api.parallelactor;

//// TODO: This is not suitable for implementing this interface. Even if you want to implement it,
// you need to distinguish between instance and non instance when calling task ()
///
/// What we need to implement is instance, and then parallelactor.task () calls on an InstanceCaller
public class ParallelInstance<A> implements ParallelActorCall<A> {

  private ParallelActor<A> parallelActor;

  private int currentIndex;

  ParallelInstance(ParallelActor<A> parallelActor, int index) {
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
