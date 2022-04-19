package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ParallelActorTaskCaller<R> {

  private ParallelActorInstance instance;

  private RayFuncR<R> func;

  private Object[] args;

  public ParallelActorTaskCaller(ParallelActorInstance instance, RayFuncR<R> func, Object[] args) {
    this.instance = instance;
    this.func = func;
    this.args = args;
  }

  public ObjectRef<R> remote() {
    ParallelActorContext ctx = Ray.internal().getParallelContext();
    return ctx.submitTask(instance.getActor(), instance.getIndex(), func, args);
  }
}
