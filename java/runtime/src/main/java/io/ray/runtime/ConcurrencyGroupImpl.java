package io.ray.runtime;

import io.ray.api.Ray;
import io.ray.api.concurrencygroup.ConcurrencyGroup;
import io.ray.api.function.RayFunc;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.RayFunction;
import java.util.ArrayList;
import java.util.List;

public class ConcurrencyGroupImpl implements ConcurrencyGroup {

  private String name;

  private int maxConcurrency;

  private List<FunctionDescriptor> functionDescriptors = new ArrayList<>();

  public ConcurrencyGroupImpl(String name, int maxConcurrency, List<RayFunc> funcs) {
    this.name = name;
    this.maxConcurrency = maxConcurrency;
    // Convert methods to function descriptors for actor method concurrency groups.
    funcs.forEach(
        func -> {
          RayFunction rayFunc =
              ((RayRuntimeInternal) Ray.internal())
                  .getFunctionManager()
                  .getFunction(Ray.getRuntimeContext().getCurrentJobId(), func);
          functionDescriptors.add(rayFunc.getFunctionDescriptor());
        });
  }

  public ConcurrencyGroupImpl(String name, int maxConcurrency) {
    this.name = name;
    this.maxConcurrency = maxConcurrency;
  }

  public void addJavaFunctionDescriptor(JavaFunctionDescriptor javaFunctionDescriptor) {
    functionDescriptors.add(javaFunctionDescriptor);
  }

  public int getMaxConcurrency() {
    return maxConcurrency;
  }

  public List<FunctionDescriptor> getFunctionDescriptors() {
    return functionDescriptors;
  }

  public String getName() {
    return name;
  }
}
