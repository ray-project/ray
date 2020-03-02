package org.ray.runtime.functionmanager;

import java.util.Arrays;
import java.util.List;
import org.ray.runtime.generated.Common.Language;

/**
 * Represents metadata of a Python function.
 */
public class PyFunctionDescriptor implements FunctionDescriptor {

  public String moduleName;

  public String className;

  public String functionName;

  public PyFunctionDescriptor(String moduleName, String className, String functionName) {
    this.moduleName = moduleName;
    this.className = className;
    this.functionName = functionName;
  }

  @Override
  public String toString() {
    return moduleName + "." + className + "." + functionName;
  }

  @Override
  public List<String> toList() {
    return Arrays.asList(moduleName, className, functionName, "" /* function hash */);
  }

  @Override
  public Language getLanguage() {
    return Language.PYTHON;
  }
}

