package io.ray.runtime.utils.parallelactor;

import com.google.common.base.Preconditions;
import io.ray.api.Ray;
import io.ray.api.parallelactor.ParallelActorExecutor;
import io.ray.api.parallelactor.ParallelStrategy;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelActorExecutorImpl extends ParallelActorExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelActorExecutorImpl.class);

  private FunctionManager functionManager = null;

  /// This should be thread safe.
  private ConcurrentHashMap<Integer, Object> instances = new ConcurrentHashMap<>();

  public ParallelActorExecutorImpl(
      ParallelStrategy strategy, int parallelNum, JavaFunctionDescriptor javaFunctionDescriptor) {
    super(strategy, parallelNum);

    functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();

    RayFunction init =
        functionManager.getFunction(
            Ray.getRuntimeContext().getCurrentJobId(), javaFunctionDescriptor);
    Thread.currentThread().setContextClassLoader(init.classLoader);
    try {
      for (int i = 0; i < parallelNum; ++i) {
        Object instance = init.getMethod().invoke(null, null);
        instances.put(i, instance);
      }
    } catch (Exception e) {
      ////
      throw new RuntimeException(e);
    } finally {
      ////
    }
  }

  public Object execute(
      int instanceIndex, JavaFunctionDescriptor functionDescriptor, Object[] args) {
    RayFunction func =
        functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), functionDescriptor);
    try {
      Preconditions.checkState(instances.containsKey(instanceIndex));
      return func.getMethod().invoke(instances.get(instanceIndex), args);
    } catch (Throwable a) {
      throw new RuntimeException(a);
    }
  }
}
