package io.ray.serve.util;

import java.lang.reflect.Method;
import org.objectweb.asm.Type;

/** tools for getting method signature */
public class SignatureUtil {
  public static final String SIGNATURE_SEP = "#";

  public static String getSignature(Method m) {
    return m.getName() + SIGNATURE_SEP + Type.getType(m).getDescriptor();
  }
}
