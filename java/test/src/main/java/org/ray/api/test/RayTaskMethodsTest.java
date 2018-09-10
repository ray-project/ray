package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.runtime.functionmanager.RayMethod;
import org.ray.runtime.functionmanager.RayTaskMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RayTaskMethodsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayTaskMethodsTest.class);

  private static class Foo {

    public Foo() {}

    public Foo(int x) {}

    public static void f1() {}

    public void f2() {}
  }

  @Test
  public void testTask() {
    RayTaskMethods methods = RayTaskMethods
        .fromClass(Foo.class.getName(), Foo.class.getClassLoader());
    LOGGER.info(methods.toString());
    int numMethods = 0;
    int numConstructors = 0;
    for (RayMethod m : methods.functions.values()) {
      if (m.isConstructor()) {
        numConstructors += 1;
      } else {
        numMethods += 1;
      }
    }
    Assert.assertEquals(numMethods, 1);
    Assert.assertEquals(numConstructors, 2);
  }
}
