package org.ray.runtime.functionmanager;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.ray.runtime.generated.Common.Language;

/**
 * Represents metadata of Java function.
 */
public final class JavaFunctionDescriptor implements FunctionDescriptor {

  /**
   * Function's class name.
   */
  public final String className;
  /**
   * Function's name.
   */
  public final String name;
  /**
   * Function's type descriptor.
   */
  public final String typeDescriptor;

  public JavaFunctionDescriptor(String className, String name, String typeDescriptor) {
    this.className = className;
    this.name = name;
    this.typeDescriptor = typeDescriptor;
  }

  @Override
  public String toString() {
    return className + "." + name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JavaFunctionDescriptor that = (JavaFunctionDescriptor) o;
    return Objects.equal(className, that.className) &&
        Objects.equal(name, that.name) &&
        Objects.equal(typeDescriptor, that.typeDescriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(className, name, typeDescriptor);
  }

  @Override
  public List<String> toList() {
    return ImmutableList.of(className, name, typeDescriptor);
  }

  @Override
  public Language getLanguage() {
    return Language.JAVA;
  }
}
