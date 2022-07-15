package io.ray.api.parallelactor;

import io.ray.api.ActorHandle;

/** The handle to a parallel actor. */
public interface ParallelActorHandle<A> {

  /** Get an execution instance of the parallel actor by the given instance ID. */
  ParallelActorInstance<A> getInstance(int instanceId);

  /** Get the parallelism of this parallel actor. */
  int getParallelism();

  /** Get the real actor handle to use. */
  ActorHandle<?> getHandle();
}
