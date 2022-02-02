package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;
import io.ray.api.options.CallOptions;

/** A helper to call java actor method which doesn't have a return value. */
public class VoidActorTaskCaller {
  private final ActorHandle actor;
  private final RayFuncVoid func;
  private final Object[] args;
  private CallOptions.Builder builder = new CallOptions.Builder();

  public VoidActorTaskCaller(ActorHandle actor, RayFuncVoid func, Object[] args) {
    this.actor = actor;
    this.func = func;
    this.args = args;
  }

  public VoidActorTaskCaller setConcurrencyGroup(String name) {
    builder.setConcurrencyGroupName(name);
    return self();
  }

  private VoidActorTaskCaller self() {
    return this;
  }

  /** Execute a function remotely. */
  public void remote() {
    Ray.internal().callActor(actor, func, args, builder.build());
  }
}
