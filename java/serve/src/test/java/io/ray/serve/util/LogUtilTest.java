package io.ray.serve.util;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LogUtilTest {

  @Test
  public void formatTest() {
    String result = LogUtil.format("{},{},{}", "1", "2", "3");
    Assert.assertEquals(result, "1,2,3");
  }
}
