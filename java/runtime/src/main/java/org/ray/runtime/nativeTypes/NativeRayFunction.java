package org.ray.runtime.nativeTypes;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;

public class NativeRayFunction {
  public int workerLanguage;
  public List<String> functionDescriptor;

  public NativeRayFunction(FunctionDescriptor functionDescriptor) {
    Preconditions.checkNotNull(functionDescriptor);
    if (functionDescriptor instanceof JavaFunctionDescriptor) {
      workerLanguage = 1;
    } else if (functionDescriptor instanceof PyFunctionDescriptor) {
      workerLanguage = 0;
    } else {
      throw new IllegalArgumentException("Unknown function descriptor type: " + functionDescriptor.getClass());
    }
    this.functionDescriptor = functionDescriptor.toList();
  }
}