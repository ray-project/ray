package io.ray.runtime.functionmanager;

import com.google.common.base.Objects;
import io.ray.runtime.generated.Common.Language;
import java.util.Arrays;
import java.util.List;

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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PyFunctionDescriptor that = (PyFunctionDescriptor) o;
    return Objects.equal(moduleName, that.moduleName) &&
      Objects.equal(className, that.className) &&
      Objects.equal(functionName, that.functionName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(moduleName, className, functionName);
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

