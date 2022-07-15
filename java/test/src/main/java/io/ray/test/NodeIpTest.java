package io.ray.test;

import io.ray.api.Ray;
import org.apache.commons.lang3.SystemUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class NodeIpTest extends BaseTest {

  private static final String NODE_IP = "127.0.0.2";

  @BeforeClass
  public void setUp() {
    if (SystemUtils.IS_OS_MAC) {
      throw new SkipException("Skip NodeIpTest on Mac OS");
    }
    System.setProperty("ray.head-args.0", "--node-ip-address=127.0.0.2");
    System.setProperty("ray.node-ip", "127.0.0.2");
  }

  static String getNodeIp() {
    return TestUtils.getRuntime().getRayConfig().nodeIp;
  }

  public void testNodeIp() {
    // this is on the driver node, and it should be equal with ray.node-ip
    String nodeIP = TestUtils.getRuntime().getRayConfig().nodeIp;
    Assert.assertEquals(nodeIP, NODE_IP);

    // this is on the worker node, and it should be equal with node-ip-address
    nodeIP = Ray.task(NodeIpTest::getNodeIp).remote().get();
    Assert.assertEquals(nodeIP, NODE_IP);
  }
}
