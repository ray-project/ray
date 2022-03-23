package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.ConcurrencyGroupBuilder;
import io.ray.api.function.RayFuncR;

public class ParallelActorCreator<A> {

  private ParallelStrategy strategy = ParallelStrategy.ALWAYS_FIRST;

  private int parallelNum = 1;

  private RayFuncR<A> func;

  private Object[] args;

  public ParallelActorCreator(RayFuncR<A> func, Object[] args) {
//    ObjectRef
    this.func = func;
    this.args = args;
  }

  public ParallelActorCreator<A> setParallels(ParallelStrategy strategy, int parallelNum) {
    this.strategy = strategy;
    this.parallelNum = parallelNum;
    return this;
  }

  public ParallelActor<A> remote() {
    ParallelContext ctx = Ray.internal().getParallelContext();
    return ctx.createParallelActorExecutor(strategy, parallelNum, this.func);
  }

}
