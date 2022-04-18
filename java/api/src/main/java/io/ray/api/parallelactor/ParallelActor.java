package io.ray.api.parallelactor;

import io.ray.api.ActorHandle;

/**
 * A `ParallelActor` could be used for creating multiple execution instances in different threads,
 * to execute the methods.
 *
 * <p>Every thread holds an instance to `A`, and the methods will be invoked on the instance which
 * is calculated according to the parallel strategy.
 *
 * <p>The parallel strategy are only working on the caller side, which means it doesn't be
 * coordinated when 2 callers to invoke 1 parallel actor.
 */
public interface ParallelActor<A> {

  /** Get an execution instance of the parallel actor by the given index. */
  ParallelInstance<A> getInstance(int index);

  /** Get the parallel number of this parallel actor. */
  int getParallelNum();

  /** Get the real actor handle to use. */
  ActorHandle<?> getHandle();
}
