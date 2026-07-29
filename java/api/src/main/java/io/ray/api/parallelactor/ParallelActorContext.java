package io.ray.api.parallelactor;

import io.ray.api.ObjectRef;
import io.ray.api.function.RayFunc;
import io.ray.api.function.RayFuncR;

public interface ParallelActorContext {

  <A> ParallelActorHandle<A> createParallelActorExecutor(int parallelism, RayFuncR<A> ctorFunc);

  <A, R> ObjectRef<R> submitTask(
      ParallelActorHandle<A> parallelActorHandle, int instanceId, RayFunc func, Object[] args);
}
