package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ParallelActorCreator<A> {

  private int parallelism = 1;

  private RayFuncR<A> func;

  private Object[] args;

  public ParallelActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  public ParallelActorCreator<A> setParallelism(int parallelism) {
    this.parallelism = parallelism;
    return this;
  }

  public ParallelActorHandle<A> remote() {
    ParallelActorContext ctx = Ray.internal().getParallelActorContext();
    return ctx.createParallelActorExecutor(parallelism, this.func);
  }
}
