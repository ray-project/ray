package org.ray.util;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.WeakHashMap;

/**
 * see http://cr.openjdk.java.net/~briangoetz/lambda/lambda-translation.html
 */
public final class LambdaUtils {

  /* use ThreadLocal to avoid lock. </br>
   * the lamda class is dymainc create, maybe a lot of this named like
   * CLASSNAME$$Lambda$1/2104457164.
   * use WeakHashMap avoid oom
   */
  private static final ThreadLocal<WeakHashMap<Class<Serializable>, SerializedLambda>>
      REPLACE_METHOD_MAPS
      = ThreadLocal.withInitial(() -> new WeakHashMap<>());

  private LambdaUtils() {
  }


  public static SerializedLambda getSerializedLambda(Serializable lambda) {
    return getSerializedLambda(lambda, false);
  }

  public static SerializedLambda getSerializedLambda(Serializable lambda, boolean forceNew) {
    if (forceNew) {
      return get(lambda);
    }
    Class<Serializable> clazz = (Class<Serializable>) lambda.getClass();
    WeakHashMap<Class<Serializable>, SerializedLambda> map = REPLACE_METHOD_MAPS.get();
    SerializedLambda slambda = map.get(clazz);
    try {
      if (slambda == null) {
        slambda = get(lambda);
        map.put(clazz, slambda);
      }
      return slambda;
    } catch (Exception e) {
      throw new RuntimeException("failed to getSerializedLambda:" + clazz.getName(), e);
    }
  }

  private static SerializedLambda get(Serializable lambda) {
    // the class of lambda which isAssignableFrom Serializable has an privte method:writeReplace
    try {
      Method m = lambda.getClass().getDeclaredMethod("writeReplace");
      m.setAccessible(true);
      return (SerializedLambda) m.invoke(lambda);
    } catch (Exception e) {
      throw new RuntimeException("failed to getSerializedLambda:" + lambda.getClass().getName(), e);
    }
  }
}