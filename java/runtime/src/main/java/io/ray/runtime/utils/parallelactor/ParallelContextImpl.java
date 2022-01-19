package io.ray.runtime.utils.parallelactor;

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.function.RayFuncR;
import io.ray.api.utils.parallelactor.ParallelActor;
import io.ray.api.utils.parallelactor.ParallelActorExecutor;
import io.ray.api.utils.parallelactor.ParallelContext;
import io.ray.api.utils.parallelactor.ParallelStrategy;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;

public class ParallelContextImpl implements ParallelContext {

  @Override
  public <A> ParallelActor<A> createParallelActorExecutor(ParallelStrategy strategy, int parallelNum, ConcurrencyGroup[] concurrencyGroups, RayFuncR<A> ctorFunc) {
    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), ctorFunc).getFunctionDescriptor();
    ActorHandle<ParallelActorExecutorImpl> parallelExecutorHandle = Ray.actor(ParallelActorExecutorImpl::new, strategy, parallelNum, functionDescriptor)
      .setConcurrencyGroups(concurrencyGroups)
      .remote();

    return new ParallelActor<>(strategy, parallelNum, parallelExecutorHandle);
  }

  @Override
  public <R> ObjectRef<R> submitParallelTask(ParallelActor parallelActor, int parallelIndex, RayFuncR func, Object[] args) {
    ActorHandle<ParallelActorExecutorImpl> parallelExecutor = parallelActor.getParallelExecutorHandle();
    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), func).getFunctionDescriptor();
    ObjectRef<Object> ret = parallelExecutor.task(ParallelActorExecutorImpl::execute, parallelActor.getNextIndex(), functionDescriptor, args).remote();
    return (ObjectRef<R>) ret;
  }
}
