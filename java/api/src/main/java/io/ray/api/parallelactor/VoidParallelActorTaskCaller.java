package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncVoid;

// TODO: test void call.
public class VoidParallelActorTaskCaller {

  private ParallelInstance instance;
  private ParallelActor parallelActor;

  private RayFuncVoid func;

  private Object[] args;

  public VoidParallelActorTaskCaller(ParallelActor parallelActor, RayFuncVoid func, Object[] args) {
    this.parallelActor = parallelActor;
    this.func = func;
    this.args = args;
  }

  public VoidParallelActorTaskCaller(ParallelInstance instance, RayFuncVoid func, Object[] args) {
    this.instance = instance;
    this.func = func;
    this.args = args;
  }

  public void remote() {
    ParallelContext ctx = Ray.internal().getParallelContext();
    if (instance != null) {
      ctx.submitTask(instance.getActor(), instance.getIndex(), func, args);
    } else {
      ctx.submitTask(parallelActor, parallelActor.getStrategy().getNextIndex(), func, args);
    }
  }
}
