package org.ray.api.test;


import java.lang.reflect.Method;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.function.RayFunc3;
import org.ray.util.MethodId;
import org.ray.util.logger.RayLog;

public class MethodIdTest {

  public static <T0, T1, T2, R0> MethodId fromLambda(RayFunc3<T0, T1, T2, R0> f) {
    MethodId mid = MethodId.fromSerializedLambda(f, true);
    return mid;
  }

  public static MethodId fromClass(Method method) {
    return MethodId.fromMethod(method);
  }

  @Test
  public void testMethodId2From() throws Exception {
    MethodId m1 = fromLambda(MethodIdTest::call);
    Method m = MethodIdTest.class.getDeclaredMethod("call", new Class[]{long.class, String.class});
    MethodId m2 = fromClass(m);
    RayLog.core.info(m1.toString());
    Assert.assertEquals(m1, m2);
  }

  public String call(long v, String s) {
    for (int i = 0; i < 100; i++) {
      v += i;
    }
    RayLog.core.info("call:" + v);
    return String.valueOf(v);
  }
}