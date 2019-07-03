package org.ray.runtime.proxyTypes;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.runtime.functionmanager.FunctionDescriptor;

public class RayFunctionProxy {
  public int language;
  public List<String> functionDescriptor;

  public RayFunctionProxy(FunctionDescriptor functionDescriptor) {
    Preconditions.checkNotNull(functionDescriptor);
    language = functionDescriptor.getLanguage();
    this.functionDescriptor = functionDescriptor.toList();
  }
}