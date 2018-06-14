package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.spi.model.RayActorMethods;
import org.ray.util.logger.RayLog;

public class RayActorMethodsTest {

  @Test
  public void testActor() throws Exception {
    RayActorMethods methods = RayActorMethods
        .fromClass(ActorTest.Adder.class.getName(), RayActorMethodsTest.class.getClassLoader());
    RayLog.core.info(methods.toString());
    Assert.assertEquals(methods.functions.size(), 5);
    Assert.assertEquals(methods.staticFunctions.size(), 1);

    RayActorMethods methods2 = RayActorMethods
        .fromClass(ActorTest.Adder2.class.getName(), RayActorMethodsTest.class.getClassLoader());
    RayLog.core.info(methods2.toString());

    Assert.assertEquals(methods2.functions.size(), 9);
    Assert.assertEquals(methods2.staticFunctions.size(), 1);
  }
}