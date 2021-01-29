package io.ray.test;

import io.ray.api.Ray;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"additional"})
public class NodeIpTest extends BaseTest{

  private static final String NODE_IP = "127.0.0.2";

  static String getNodeIp() {
    return TestUtils.getRuntime().getRayConfig().nodeIp;
  }

  public void testNodeIp() {
    String node_ip = TestUtils.getRuntime().getRayConfig().nodeIp;
    Assert.assertEquals(node_ip, NODE_IP);

    node_ip = Ray.task(NodeIpTest::getNodeIp).remote().get();
    Assert.assertEquals(node_ip, NODE_IP);
  }
}
