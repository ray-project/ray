package io.ray.runtime.utils.parallelactor;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.concurrencygroup.ConcurrencyGroupBuilder;
import io.ray.api.function.RayFuncR;
import io.ray.api.parallelactor.*;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.utils.parallelactor.strategy.AlwaysFirstStrategy;
import io.ray.runtime.utils.parallelactor.strategy.RoundRobinStrategy;

public class ParallelContextImpl implements ParallelContext {

  @Override
  public <A> ParallelActor<A> createParallelActorExecutor(
      ParallelStrategyInterface strategy, RayFuncR<A> ctorFunc) {
    int parallelNum = strategy.getParallelNum();
    ConcurrencyGroup[] concurrencyGroups = new ConcurrencyGroup[parallelNum];
    for (int i = 0; i < parallelNum; ++i) {
      concurrencyGroups[i] =
          new ConcurrencyGroupBuilder<ParallelActorExecutor>()
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
        Ray.actor(
                ParallelActorExecutorImpl::new,
                strategy.getStrategyEnum(),
                parallelNum,
                functionDescriptor)
            .setConcurrencyGroups(concurrencyGroups)
            .remote();

    return new ParallelActorImpl<>(strategy, parallelExecutorHandle);
  }

  @Override
  public <R> ObjectRef<R> submitTask(
      ParallelActor parallelActor, int instanceIndex, RayFuncR func, Object[] args) {
    ActorHandle<ParallelActorExecutorImpl> parallelExecutor = parallelActor.getExecutor();
    FunctionManager functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    JavaFunctionDescriptor functionDescriptor =
        functionManager
            .getFunction(Ray.getRuntimeContext().getCurrentJobId(), func)
            .getFunctionDescriptor();
    ObjectRef<Object> ret =
        parallelExecutor
            .task(ParallelActorExecutorImpl::execute, instanceIndex, functionDescriptor, args)
            .remote();
    return (ObjectRef<R>) ret;
  }

  @Override
  public ParallelStrategyInterface createParallelStrategy(
      ParallelStrategy strategyEnum, int parallelNum) {
    Preconditions.checkNotNull(strategyEnum);
    if (ParallelStrategy.ALWAYS_FIRST.equals(strategyEnum)) {
      return new AlwaysFirstStrategy(parallelNum);
    } else if (ParallelStrategy.ROUND_ROBIN.equals(strategyEnum)) {
      return new RoundRobinStrategy(parallelNum);
    }
    throw new RuntimeException(
        String.format("Not implemented parallel strategy: %s", strategyEnum));
  }
}
