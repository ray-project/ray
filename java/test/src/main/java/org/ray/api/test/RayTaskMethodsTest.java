package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.spi.model.RayTaskMethods;
import org.ray.util.logger.RayLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RayTaskMethodsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayTaskMethodsTest.class);

//  @Test
  public void testTask() {
    RayTaskMethods methods = RayTaskMethods
        .fromClass(EchoTest.class.getName(), RayTaskMethodsTest.class.getClassLoader());
    LOGGER.info(methods.toString());
    Assert.assertEquals(methods.functions.size(), 3);
  }
}