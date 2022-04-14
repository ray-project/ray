package io.ray.api.parallelactor;

import io.ray.api.ActorHandle;

public interface ParallelActor<A> extends ParallelActorCall<A> {

  ParallelInstance<A> getInstance(int index);

  ParallelStrategyInterface getStrategy();

  ActorHandle<?> getHandle();
}
