package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;

public class ParallelActorCreator<A> {

  private ParallelStrategyInterface strategy = null;

  private RayFuncR<A> func;

  private Object[] args;

  public ParallelActorCreator(RayFuncR<A> func, Object[] args) {
    //    ObjectRef
    this.func = func;
    this.args = args;

    // By default, it should be ALWAYS_FIRST with 1 parallel.
    ParallelContext ctx = Ray.internal().getParallelContext();
    this.strategy = ctx.createParallelStrategy(ParallelStrategy.ALWAYS_FIRST, 1);
  }

  public ParallelActorCreator<A> setParallels(ParallelStrategy strategy, int parallelNum) {
    ParallelContext ctx = Ray.internal().getParallelContext();
    this.strategy = ctx.createParallelStrategy(strategy, parallelNum);
    return this;
  }

  public ParallelActor<A> remote() {
    ParallelContext ctx = Ray.internal().getParallelContext();
    return ctx.createParallelActorExecutor(strategy, this.func);
  }
}
