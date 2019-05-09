package org.ray.api.test;

import com.google.common.base.Preconditions;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.TestUtils;
import org.ray.api.runtimecontext.NodeInfo;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.gcs.GcsClient;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class GcsClientTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.resources", "A:8");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.resources");
  }

  @Test
  public void testGetAllNodeInfo() {
    TestUtils.skipTestUnderSingleProcess();
    RayConfig config = ((AbstractRayRuntime)Ray.internal()).getRayConfig();

    Preconditions.checkNotNull(config);
    GcsClient gcsClient = ((AbstractRayRuntime)Ray.internal()).getGcsClient();
    List<NodeInfo> allNodeInfo = gcsClient.getAllNodeInfo();
    Assert.assertEquals(allNodeInfo.size(), 1);
    Assert.assertEquals(allNodeInfo.get(0).nodeAddress, config.nodeIp);
    Assert.assertTrue(allNodeInfo.get(0).isAlive);
    Assert.assertEquals(allNodeInfo.get(0).resources.get("A"), 8.0);
  }

}
