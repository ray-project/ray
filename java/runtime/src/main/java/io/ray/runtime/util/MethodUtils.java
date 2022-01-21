package io.ray.runtime.util;

import io.ray.api.Ray;
import io.ray.runtime.RayRuntimeInternal;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public final class MethodUtils {

  public static String getSignature(Method m) {
    String sig;
    try {
      Field signatureField = Method.class.getDeclaredField("signature");
      signatureField.setAccessible(true);
      sig = (String) signatureField.get(m);
      if (sig != null) {
        return sig;
      }
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    StringBuilder sb = new StringBuilder("(");
    for (Class<?> c : m.getParameterTypes()) {
      sb.append((sig = Array.newInstance(c, 0).toString()).substring(1, sig.indexOf('@')));
    }

    return sb.append(')')
        .append(
            m.getReturnType() == void.class
                ? "V"
                : (sig = Array.newInstance(m.getReturnType(), 0).toString())
                    .substring(1, sig.indexOf('@')))
        .toString();
  }

  public static Class<?> getReturnTypeFromSignature(String signature) {
    final int startIndex = signature.indexOf(')');
    final int endIndex = signature.lastIndexOf(';');
    final String className = signature.substring(startIndex + 2, endIndex).replace('/', '.');
    Class<?> actorClz;
    try {
      try {
        actorClz = Class.forName(className);
      } catch (ClassNotFoundException e) {
        /// This code path indicates that here might be in another thread of a worker.
        /// So try to load the class from URLClassLoader of this worker.
        ClassLoader cl =
            ((RayRuntimeInternal) Ray.internal()).getWorkerContext().getCurrentClassLoader();
        actorClz = Class.forName(className, true, cl);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return actorClz;
  }
}
