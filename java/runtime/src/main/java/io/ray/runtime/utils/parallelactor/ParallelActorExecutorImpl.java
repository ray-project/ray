package io.ray.runtime.utils.parallelactor;

import io.ray.api.Ray;
import io.ray.api.parallelactor.ParallelActorExecutor;
import io.ray.api.parallelactor.ParallelStrategy;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ParallelActorExecutorImpl extends ParallelActorExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelActorExecutorImpl.class);

  private FunctionManager functionManager = null;

  private List<Object> instances = new ArrayList<>();

  public ParallelActorExecutorImpl(ParallelStrategy strategy, int parallelNum, JavaFunctionDescriptor javaFunctionDescriptor) {
    super(strategy, parallelNum);

    functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();

    RayFunction init = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), javaFunctionDescriptor);
    Thread.currentThread().setContextClassLoader(init.classLoader);
    try {
      for (int i = 0; i < parallelNum; ++i) {
        Object instance = init.getMethod().invoke(null, null);
        instances.add(instance);
      }
    } catch (Exception e) {
      ////
      throw new RuntimeException(e);
    } finally {
      ////
    }

  }

  public Object execute(int instanceIndex, JavaFunctionDescriptor functionDescriptor,  Object[] args) {
    RayFunction func = functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), functionDescriptor);
    try {
      if (instances.get(instanceIndex) == null) {
      }

      return func.getMethod().invoke(instances.get(instanceIndex), args);
    } catch (Throwable a) {
      throw new RuntimeException(a);
    }
  }

}
