package io.ray.api.parallelactor;

import io.ray.api.ActorHandle;

/**
 * A `ParallelActorHandle` could be used for creating multiple execution instances in different
 * threads, to execute the methods.
 *
 * <p>Every thread holds an instance to `A`, and the methods will be invoked on the instance in
 * different threads.
 */
public interface ParallelActorHandle<A> {

  /** Get an execution instance of the parallel actor by the given index. */
  ParallelActorInstance<A> getInstance(int index);

  /** Get the parallelism of this parallel actor. */
  int getParallelism();

  /** Get the real actor handle to use. */
  ActorHandle<?> getHandle();
}
