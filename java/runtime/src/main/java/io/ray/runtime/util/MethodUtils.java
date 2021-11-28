package io.ray.runtime.util;

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
}
