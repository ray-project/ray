package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.api.annotation.RayRemote;
import org.ray.spi.model.RayActorMethods;
import org.ray.util.logger.RayLog;

public class RayActorMethodsTest {

  @RayRemote
  public static class ExampleActor {

    public void func1() {}

    public void func2() {}

    public static void func3() {}
  }

  @Test
  public void testActorMethods() {
    RayActorMethods methods = RayActorMethods
        .fromClass(ExampleActor.class.getName(), RayActorMethodsTest.class.getClassLoader());
    RayLog.core.info(methods.toString());
    Assert.assertEquals(methods.functions.size(), 2);
    Assert.assertEquals(methods.staticFunctions.size(), 1);
  }
}
