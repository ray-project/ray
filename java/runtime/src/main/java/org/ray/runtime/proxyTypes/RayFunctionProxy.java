package org.ray.runtime.proxyTypes;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.runtime.functionmanager.FunctionDescriptor;

public class RayFunctionProxy {
  public int workerLanguage;
  public List<String> functionDescriptor;

  public RayFunctionProxy(FunctionDescriptor functionDescriptor) {
    Preconditions.checkNotNull(functionDescriptor);
    workerLanguage = functionDescriptor.getLanguage().getValue();
    this.functionDescriptor = functionDescriptor.toList();
  }
}