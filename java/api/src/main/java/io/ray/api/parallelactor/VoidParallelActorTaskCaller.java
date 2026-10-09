package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;

public class VoidParallelActorTaskCaller {

  private ParallelActorInstance instance;

  private RayFuncVoid func;

  private Object[] args;

  public VoidParallelActorTaskCaller(
      ParallelActorInstance instance, RayFuncVoid func, Object[] args) {
    this.instance = instance;
    this.func = func;
    this.args = args;
  }

  public void remote() {
    ParallelActorContext ctx = Ray.internal().getParallelActorContext();
    ctx.submitTask(instance.getActor(), instance.getInstanceId(), func, args);
  }
}
