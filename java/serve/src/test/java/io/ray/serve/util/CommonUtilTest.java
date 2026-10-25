package io.ray.serve.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CommonUtilTest {

  public static class InnerTest {}

  @Test
  public void getSimpleClassNameTest() {
    String name = CommonUtil.getDeploymentName(InnerTest.class.getName());
    Assert.assertEquals(name, "InnerTest");
  }
}
