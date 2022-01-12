package io.ray.api.utils.parallelactor;

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
    ConcurrencyGroup[] concurrencyGroups = new ConcurrencyGroup[parallelNum];
    for (int i = 0; i < parallelNum; ++i) {
      concurrencyGroups[i] = new ConcurrencyGroupBuilder<ParallelActorExecutor>()
        .setName(String.format("PARALLEL_INSTANCE_%d", i))
        .setMaxConcurrency(1)
        .build();
    }

    ParallelContext ctx = Ray.internal().getParallelContext();
    return ctx.createParallelActorExecutor(strategy, parallelNum, concurrencyGroups, this.func);
  }

}
