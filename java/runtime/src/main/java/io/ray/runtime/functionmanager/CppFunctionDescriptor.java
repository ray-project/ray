package io.ray.runtime.functionmanager;

import com.google.common.base.Objects;
import io.ray.runtime.generated.Common.Language;
import java.util.Arrays;
import java.util.List;

/** Represents metadata of a Cpp function. */
public class CppFunctionDescriptor implements FunctionDescriptor {

  public String createFunctionName;

  public String className;

  public String caller;

  public CppFunctionDescriptor(String createFunctionName, String caller, String className) {
    this.createFunctionName = createFunctionName;
    this.caller = caller;
    this.className = className;
  }

  @Override
  public String toString() {
    return createFunctionName + "." + caller + "." + className;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CppFunctionDescriptor that = (CppFunctionDescriptor) o;
    return Objects.equal(createFunctionName, that.createFunctionName)
        && Objects.equal(className, that.className)
        && Objects.equal(caller, that.caller);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(createFunctionName, caller, className);
  }

  @Override
  public List<String> toList() {
    return Arrays.asList(createFunctionName, caller, className);
  }

  @Override
  public Language getLanguage() {
    return Language.CPP;
  }
}
