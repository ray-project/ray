package org.ray.api.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ray.api.Ray;
import org.ray.api.RayRemote;

@RunWith(MyRunner.class)
public class RpcTest {

  @RayRemote
  public static Integer with0Params() {
    return 0;
  }

  @RayRemote
  public static Integer with1Params(Integer x) {
    return x;
  }

  @RayRemote
  public static Integer with2Params(Integer x, Integer y) {
    return x + y;
  }

  @RayRemote
  public static Integer with3Params(Integer x, Integer y, Integer z) {
    return x + y + z;
  }

  @Test
  public void test() {
    Assert.assertEquals(0, (int) Ray.call(RpcTest::with0Params).get());
    Assert.assertEquals(1, (int) Ray.call(RpcTest::with1Params, 1).get());
    Assert.assertEquals(3, (int) Ray.call(RpcTest::with2Params, 1, 2).get());
    Assert.assertEquals(6, (int) Ray.call(RpcTest::with3Params, 1, 2, 3).get());
  }
}
