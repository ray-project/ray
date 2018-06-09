package org.ray.spi.model;

import java.lang.reflect.Method;

/**
 * method info.
 */
public class RayMethod {

  public final Method invokable;
  public final String fullName;
  // TODO: other annotated information

  public RayMethod(Method m) {
    invokable = m;
    fullName = m.getDeclaringClass().getName() + "." + m.getName();
  }

  public void check() {
    for (Class<?> paramCls : invokable.getParameterTypes()) {
      if (paramCls.isPrimitive()) {
        throw new RuntimeException(
            "@RayRemote function " + fullName + " must have all non-primitive typed parameters");
      }
    }
  }
}
