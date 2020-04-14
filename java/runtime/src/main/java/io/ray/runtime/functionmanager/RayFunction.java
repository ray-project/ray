package io.ray.runtime.functionmanager;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Represents a Ray function (either a Method or a Constructor in Java) and its metadata.
 */
public class RayFunction {

  /**
   * The executor object, can be either a Method or a Constructor.
   */
  public final Executable executable;

  /**
   * This function's class loader.
   */
  public final ClassLoader classLoader;

  /**
   * Function's metadata.
   */
  public final JavaFunctionDescriptor functionDescriptor;

  public RayFunction(Executable executable, ClassLoader classLoader,
      JavaFunctionDescriptor functionDescriptor) {
    this.executable = executable;
    this.classLoader = classLoader;
    this.functionDescriptor = functionDescriptor;
  }

  /**
   * @return True if it's a constructor, otherwise it's a method.
   */
  public boolean isConstructor() {
    return executable instanceof Constructor;
  }

  /**
   * @return The underlying constructor object.
   */
  public Constructor<?> getConstructor() {
    return (Constructor<?>) executable;
  }

  /**
   * @return The underlying method object.
   */
  public Method getMethod() {
    return (Method) executable;
  }

  public JavaFunctionDescriptor getFunctionDescriptor() {
    return functionDescriptor;
  }

  /**
   * @return Whether this function has a return value.
   */
  public boolean hasReturn() {
    if (isConstructor()) {
      return true;
    } else {
      return !getMethod().getReturnType().equals(void.class);
    }
  }

  /**
   * @return Return type.
   */
  public Optional<Class<?>> getReturnType() {
    if (hasReturn()) {
      return Optional.of(((Method) executable).getReturnType());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return executable.toString();
  }
}
