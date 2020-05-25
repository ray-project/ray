package io.ray.runtime.util;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

/**
 * see http://cr.openjdk.java.net/~briangoetz/lambda/lambda-translation.html.
 */
public final class LambdaUtils {

  private LambdaUtils() {
  }


  public static SerializedLambda getSerializedLambda(Serializable lambda) {
    // Note.
    // the class of lambda which isAssignableFrom Serializable
    // has an privte method:writeReplace
    // This mechanism may be changed in the future
    try {
      Method m = lambda.getClass().getDeclaredMethod("writeReplace");
      m.setAccessible(true);
      return (SerializedLambda) m.invoke(lambda);
    } catch (Exception e) {
      throw new RuntimeException("failed to getSerializedLambda:" + lambda.getClass().getName(), e);
    }
  }


}