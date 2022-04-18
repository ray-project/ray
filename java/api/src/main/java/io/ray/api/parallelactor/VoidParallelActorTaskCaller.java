package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;

public class VoidParallelActorTaskCaller {

  private ParallelInstance instance;

  private RayFuncVoid func;

  private Object[] args;

  public VoidParallelActorTaskCaller(ParallelInstance instance, RayFuncVoid func, Object[] args) {
    this.instance = instance;
    this.func = func;
    this.args = args;
  }

  public void remote() {
    ParallelContext ctx = Ray.internal().getParallelContext();
    ctx.submitTask(instance.getActor(), instance.getIndex(), func, args);
  }
}
