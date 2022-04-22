package io.ray.runtime.utils.parallelactor;

import com.google.common.base.Preconditions;
import io.ray.api.Ray;
import io.ray.runtime.RayRuntimeInternal;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelActorExecutorImpl {

  private static final Logger LOG = LoggerFactory.getLogger(ParallelActorExecutorImpl.class);

  private FunctionManager functionManager = null;

  private ConcurrentHashMap<Integer, Object> instances = new ConcurrentHashMap<>();

  public ParallelActorExecutorImpl(int parallelism, JavaFunctionDescriptor javaFunctionDescriptor)
      throws InvocationTargetException, IllegalAccessException {

    functionManager = ((RayRuntimeInternal) Ray.internal()).getFunctionManager();
    RayFunction init =
        functionManager.getFunction(
            Ray.getRuntimeContext().getCurrentJobId(), javaFunctionDescriptor);
    Thread.currentThread().setContextClassLoader(init.classLoader);
    for (int i = 0; i < parallelism; ++i) {
      Object instance = init.getMethod().invoke(null, null);
      instances.put(i, instance);
    }
  }

  public Object execute(int instanceId, JavaFunctionDescriptor functionDescriptor, Object[] args)
      throws IllegalAccessException, InvocationTargetException {
    RayFunction func =
        functionManager.getFunction(Ray.getRuntimeContext().getCurrentJobId(), functionDescriptor);
    Preconditions.checkState(instances.containsKey(instanceId));
    return func.getMethod().invoke(instances.get(instanceId), args);
  }
}
