package io.ray.api.parallelactor;

public class ParallelActorInstance<A> implements ActorCall<A> {

  private ParallelActorHandle<A> parallelActorHandle;

  private int instanceId;

  public ParallelActorInstance(ParallelActorHandle<A> parallelActorHandle, int instanceId) {
    this.parallelActorHandle = parallelActorHandle;
    this.instanceId = instanceId;
  }

  ParallelActorHandle<A> getActor() {
    return parallelActorHandle;
  }

  int getInstanceId() {
    return instanceId;
  }
}
