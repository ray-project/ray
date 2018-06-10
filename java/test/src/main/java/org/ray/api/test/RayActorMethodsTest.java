package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.ray.spi.model.RayActorMethods;

/**
 * @author lin
 * @version $Id: RayActorMethodsTest.java, v 0.1 2018年06月10日 20:00 lin Exp $
 */
public class RayActorMethodsTest {

  @Test
  public void testActor() throws Exception {
    RayActorMethods methods = RayActorMethods
        .formClass(ActorTest.Adder.class.getName(), RayActorMethodsTest.class.getClassLoader());
    System.out.println(methods);
    Assert.assertEquals(methods.functions.size(),5);
    Assert.assertEquals(methods.staticFunctions.size(),1);

    RayActorMethods methods2 = RayActorMethods
        .formClass(ActorTest.Adder2.class.getName(), RayActorMethodsTest.class.getClassLoader());
    System.out.println(methods2);

    Assert.assertEquals(methods2.functions.size(),9);
    Assert.assertEquals(methods2.staticFunctions.size(),1);
  }
}