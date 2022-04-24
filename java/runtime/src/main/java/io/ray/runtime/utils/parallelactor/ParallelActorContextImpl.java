package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.ConcurrencyGroupBuilder;
import io.ray.api.function.RayFunc;
import io.ray.api.function.RayFuncR;
import io.ray.api.parallelactor.*;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;

public class ParallelActorContextImpl implements ParallelActorContext {

  @Override
  public <A> ParallelActorHandle<A> createParallelActorExecutor(
      int parallelism, RayFuncR<A> ctorFunc) {
    ConcurrencyGroup[] concurrencyGroups = new ConcurrencyGroup[parallelism];
    for (int i = 0; i < parallelism; ++i) {
      concurrencyGroups[i] =
          new ConcurrencyGroupBuilder<ParallelActorExecutorImpl>()
              .setName(String.format("PARALLEL_INSTANCE_%d", i))
              .setMaxConcurrency(1)
              .build();
    }

    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor =
        functionManager
            .getFunction(Ray.getRuntimeContext().getCurrentJobId(), ctorFunc)
            .getFunctionDescriptor();
    ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle =
        Ray.actor(ParallelActorExecutorImpl::new, parallelism, functionDescriptor)
            .setConcurrencyGroups(concurrencyGroups)
            .remote();

    return new ParallelActorHandleImpl<>(parallelism, parallelExecutorHandle);
  }

  @Override
  public <A, R> ObjectRef<R> submitTask(
      ParallelActorHandle<A> parallelActorHandle, int instanceId, RayFunc func, Object[] args) {
    ActorHandle<ParallelActorExecutorImpl> parallelExecutor =
        ((ParallelActorHandleImpl) parallelActorHandle).getExecutor();
    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor =
        functionManager
            .getFunction(Ray.getRuntimeContext().getCurrentJobId(), func)
            .getFunctionDescriptor();
    ObjectRef<Object> ret =
        parallelExecutor
            .task(ParallelActorExecutorImpl::execute, instanceId, functionDescriptor, args)
            .setConcurrencyGroup(String.format("PARALLEL_INSTANCE_%d", instanceId))
            .remote();
    return (ObjectRef<R>) ret;
  }
}
