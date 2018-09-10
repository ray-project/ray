package org.ray.api.test;


import java.lang.reflect.Executable;
import org.junit.Assert;
import org.junit.Test;
import org.ray.api.function.RayFunc2;
import org.ray.runtime.util.MethodId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MethodIdTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(MethodIdTest.class);

  @Test
  public void testNormalMethod() throws Exception {
    RayFunc2<Integer, String, String> f = MethodIdTest::foo;
    MethodId m1 = MethodId.fromSerializedLambda(f);
    Executable e = MethodIdTest.class.getDeclaredMethod("foo", int.class, String.class);
    MethodId m2 = MethodId.fromExecutable(e);
    LOGGER.info("{}, {}", m1, m2);
    Assert.assertEquals(m1, m2);
  }

  @Test
  public void testConstructor() throws Exception {
    RayFunc2<Integer, String, Foo> f = Foo::new;
    MethodId m1 = MethodId.fromSerializedLambda(f);
    Executable e = Foo.class.getConstructor(int.class, String.class);
    MethodId m2 = MethodId.fromExecutable(e);
    LOGGER.info("{}, {}", m1, m2);
    Assert.assertEquals(m1, m2);
  }

  public static String foo(int a, String b) {
    return a + b;
  }

  public static class Foo {
    public Foo(int a, String b) {}
  }
}

