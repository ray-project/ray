package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.function.RayFuncR;

public interface ParallelContext {

  <A> ParallelActor<A> createParallelActorExecutor(
      ParallelStrategy strategy, int parallelNum, RayFuncR<A> ctorFunc);

  <R> ObjectRef<R> submitTask(
      ParallelActor parallelActor, int instanceIndex, RayFuncR func, Object[] args);
}
