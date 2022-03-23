package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ParallelActorTaskCaller<R> {

  private ParallelActor parallelActor;

  private RayFuncR<R> func;

  private Object[] args;

  public ParallelActorTaskCaller(ParallelActor parallelActor, RayFuncR<R> func, Object[] args) {
    this.parallelActor = parallelActor;
    this.func = func;
    this.args = args;
  }

  public ObjectRef<R> remote() {

    ParallelContext ctx = Ray.internal().getParallelContext();
    /// TODO(qwang): Select a instance index.
    ObjectRef<R> ret = ctx.submitTask(parallelActor, 0, func, args);
    return (ObjectRef<R>) ret;
  }

}
