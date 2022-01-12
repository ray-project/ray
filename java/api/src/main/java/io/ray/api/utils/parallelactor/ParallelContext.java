package io.ray.api.utils.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.function.RayFuncR;

public interface ParallelContext {

  <A> ParallelActor<A> createParallelActorExecutor(ParallelStrategy strategy, int parallelNum, ConcurrencyGroup[] concurrencyGroups, RayFuncR<A> ctorFunc);

  ObjectRef<Object> submitParallelTask(ParallelActor parallelActor, int parallelIndex, RayFuncR func, Object[] args);
}
