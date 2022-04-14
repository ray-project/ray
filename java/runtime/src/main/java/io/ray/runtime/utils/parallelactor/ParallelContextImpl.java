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

public class ParallelContextImpl implements ParallelContext {

  @Override
  public <A> ParallelActor<A> createParallelActorExecutor(
      ParallelStrategyInterface strategy, RayFuncR<A> ctorFunc) {
    int parallelNum = strategy.getParallelNum();
    ConcurrencyGroup[] concurrencyGroups = new ConcurrencyGroup[parallelNum];
    for (int i = 0; i < parallelNum; ++i) {
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
        Ray.actor(ParallelActorExecutorImpl::new, parallelNum, functionDescriptor)
            .setConcurrencyGroups(concurrencyGroups)
            .remote();

    return new ParallelActorImpl<>(strategy, parallelExecutorHandle);
  }

  @Override
  public <A, R> ObjectRef<R> submitTask(
      ParallelActor<A> parallelActor, int instanceIndex, RayFunc func, Object[] args) {
    ActorHandle<ParallelActorExecutorImpl> parallelExecutor =
        ((ParallelActorImpl) parallelActor).getExecutor();
    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor =
        functionManager
            .getFunction(Ray.getRuntimeContext().getCurrentJobId(), func)
            .getFunctionDescriptor();
    ObjectRef<Object> ret =
        parallelExecutor
            .task(ParallelActorExecutorImpl::execute, instanceIndex, functionDescriptor, args)
            .setConcurrencyGroup(String.format("PARALLEL_INSTANCE_%d", instanceIndex))
            .remote();
    return (ObjectRef<R>) ret;
  }
}
