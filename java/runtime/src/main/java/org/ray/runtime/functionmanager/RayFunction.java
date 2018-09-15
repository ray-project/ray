package org.ray.runtime.functionmanager;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import org.ray.api.annotation.RayRemote;

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
  public final FunctionDescriptor functionDescriptor;

  public RayFunction(Executable executable, ClassLoader classLoader,
      FunctionDescriptor functionDescriptor) {
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

  public FunctionDescriptor getFunctionDescriptor() {
    return functionDescriptor;
  }

  public RayRemote getRayRemoteAnnotation() {
    RayRemote rayRemote = executable.getAnnotation(RayRemote.class);
    if (rayRemote == null) {
      // If the method doesn't have a annotation, get the annotation from
      // its wrapping class.
      rayRemote = executable.getDeclaringClass().getAnnotation(RayRemote.class);
    }
    return rayRemote;
  }

  @Override
  public String toString() {
    return executable.toString();
  }
}
