package org.ray.runtime.nativeTypes;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.runtime.functionmanager.FunctionDescriptor;

public class NativeRayFunction {
  public int language;
  public List<String> functionDescriptor;

  public NativeRayFunction(FunctionDescriptor functionDescriptor) {
    Preconditions.checkNotNull(functionDescriptor);
    language = functionDescriptor.getLanguage().getNumber();
    this.functionDescriptor = functionDescriptor.toList();
  }
}