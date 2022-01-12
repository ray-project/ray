package io.ray.runtime.utils.parallelactor;

import io.ray.api.Ray;
import io.ray.api.utils.parallelactor.ParallelActorExecutor;
import io.ray.api.utils.parallelactor.ParallelStrategy;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;

import java.util.List;

public class ParallelActorExecutorImpl extends ParallelActorExecutor {

  private FunctionManager functionManager = null;

  private List<Object> instances = null;

  public ParallelActorExecutorImpl(ParallelStrategy strategy, int parallelNum, JavaFunctionDescriptor javaFunctionDescriptor) {
    super(strategy, parallelNum);

    functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    RayFunction init = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), javaFunctionDescriptor);

    try {
      for (int i = 0; i < parallelNum; ++i) {
        Object instance = init.getConstructor().newInstance();
        instances.add(instance);
      }
    } catch (Exception e) {
      ////
    } finally {
      ////
    }

  }

  @Override
  protected Object internalExecute(int index) {
//    RayFunction func = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), );
    return null;
  }

  public Object execute(int instanceIndex, JavaFunctionDescriptor functionDescriptor,  Object[] args) {

    functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    RayFunction func = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), functionDescriptor);
    try {
      return func.getMethod().invoke(args);
    } catch (Throwable a) {

    } finally {

    }

    return null;
  }

}
