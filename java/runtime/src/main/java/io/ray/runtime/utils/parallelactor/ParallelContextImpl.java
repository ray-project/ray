package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.ConcurrencyGroupBuilder;
import io.ray.api.function.RayFuncR;
import io.ray.api.parallelactor.ParallelActor;
import io.ray.api.parallelactor.ParallelActorExecutor;
import io.ray.api.parallelactor.ParallelContext;
import io.ray.api.parallelactor.ParallelStrategy;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;

public class ParallelContextImpl implements ParallelContext {

  @Override
  public <A> ParallelActor<A> createParallelActorExecutor(ParallelStrategy strategy, int parallelNum, RayFuncR<A> ctorFunc) {
    ConcurrencyGroup[] concurrencyGroups = new ConcurrencyGroup[parallelNum];
    for (int i = 0; i < parallelNum; ++i) {
      concurrencyGroups[i] = new ConcurrencyGroupBuilder<ParallelActorExecutor>()
        .setName(String.format("PARALLEL_INSTANCE_%d", i))
        .setMaxConcurrency(1)
        .build();
    }

    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), ctorFunc).getFunctionDescriptor();
    ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle = Ray.actor(ParallelActorExecutorImpl::new, strategy, parallelNum, functionDescriptor)
      .setConcurrencyGroups(concurrencyGroups)
      .remote();

    return new ParallelActor<>(strategy, parallelNum, parallelExecutorHandle);
  }

  @Override
  public <R> ObjectRef<R> submitTask(ParallelActor parallelActor, RayFuncR func, Object[] args) {
    ActorHandle<ParallelActorExecutorImpl> parallelExecutor = parallelActor.getExecutor();
    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), func).getFunctionDescriptor();
    ObjectRef<Object> ret = parallelExecutor.task(ParallelActorExecutorImpl::execute, functionDescriptor, args).remote();
    return (ObjectRef<R>) ret;
  }
}
