package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.spi.model.RayTaskMethods;
import org.ray.util.logger.RayLog;


public class RayTaskMethodsTest {

  @Test
  public void testTask() throws Exception {
    RayTaskMethods methods = RayTaskMethods
        .fromClass(EchoTest.class.getName(), RayTaskMethodsTest.class.getClassLoader());
    RayLog.core.info(methods.toString());
    Assert.assertEquals(methods.functions.size(), 3);
  }
}