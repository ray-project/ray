package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ParallelActorCreator<A> {

  private int parallelNum = 1;

  private RayFuncR<A> func;

  private Object[] args;

  public ParallelActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  public ParallelActorCreator<A> setParallels(int parallelNum) {
    this.parallelNum = parallelNum;
    return this;
  }

  public ParallelActor<A> remote() {
    ParallelContext ctx = Ray.internal().getParallelContext();
    return ctx.createParallelActorExecutor(parallelNum, this.func);
  }
}
