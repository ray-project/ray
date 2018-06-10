package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.spi.model.RayTaskMethods;

/**
 * @author lin
 * @version $Id: RayTaskMethodsTest.java, v 0.1 2018年06月10日 20:11 lin Exp $
 */
public class RayTaskMethodsTest {

  @Test
  public void testTask() throws Exception {
    RayTaskMethods methods = RayTaskMethods
        .formClass(EchoTest.class.getName(), RayTaskMethodsTest.class.getClassLoader());
    System.out.println(methods);
    Assert.assertEquals(methods.functions.size(), 3);
  }
}