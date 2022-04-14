package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.function.RayFunc;
import io.ray.api.function.RayFuncR;

public interface ParallelContext {

  <A> ParallelActor<A> createParallelActorExecutor(
      ParallelStrategyInterface strategy, RayFuncR<A> ctorFunc);

  <A, R> ObjectRef<R> submitTask(
    ParallelActor<A> parallelActor, int instanceIndex, RayFunc func, Object[] args);

  ParallelStrategyInterface createParallelStrategy(ParallelStrategy strategyEnum, int parallelNum);
}
