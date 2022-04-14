package io.ray.api.parallelactor;

public interface ParallelActor<A> extends ParallelActorCall<A> {

  ParallelInstance<A> getInstance(int index);

  ParallelStrategyInterface getStrategy();
}
