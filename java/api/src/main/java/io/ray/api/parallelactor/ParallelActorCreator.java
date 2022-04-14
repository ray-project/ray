package io.ray.api.parallelactor;

import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;
import io.ray.api.parallelactor.strategy.RoundRobinStrategy;

public class ParallelActorCreator<A> {

  private ParallelStrategyInterface strategy = null;

  private RayFuncR<A> func;

  private Object[] args;

  public ParallelActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;

    // By default, it should be RoundRobinStrategy with 1 parallel.
    ParallelContext ctx = Ray.internal().getParallelContext();
    this.strategy = new RoundRobinStrategy(1);
  }

  public ParallelActorCreator<A> setStrategy(ParallelStrategyInterface strategy) {
    this.strategy = strategy;
    return this;
  }

  public ParallelActor<A> remote() {
    ParallelContext ctx = Ray.internal().getParallelContext();
    return ctx.createParallelActorExecutor(strategy, this.func);
  }
}
