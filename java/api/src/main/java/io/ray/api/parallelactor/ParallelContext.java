package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.function.RayFunc;
import io.ray.api.function.RayFuncR;

public interface ParallelContext {

  <A> ParallelActorHandle<A> createParallelActorExecutor(int parallelNum, RayFuncR<A> ctorFunc);

  <A, R> ObjectRef<R> submitTask(
    ParallelActorHandle<A> parallelActorHandle, int instanceIndex, RayFunc func, Object[] args);
}
