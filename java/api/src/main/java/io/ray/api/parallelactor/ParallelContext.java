package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.function.RayFuncR;

public interface ParallelContext {

  <A> ParallelActor<A> createParallelActorExecutor(
      ParallelStrategyInterface strategy, RayFuncR<A> ctorFunc);

  <R> ObjectRef<R> submitTask(
      ParallelActor parallelActor, int instanceIndex, RayFuncR func, Object[] args);

  ParallelStrategyInterface createParallelStrategy(ParallelStrategy strategyEnum, int parallelNum);
}
