package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.id.JobId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.gcs.GcsClient;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class GcsClientTest extends BaseTest {

  @BeforeClass
  public void setUp() {
    System.setProperty("ray.resources", "A:8");
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.resources");
  }

  public void testGetAllNodeInfo() {
    RayConfig config = TestUtils.getRuntime().getRayConfig();

    Preconditions.checkNotNull(config);
    GcsClient gcsClient = TestUtils.getRuntime().getGcsClient();
    List<NodeInfo> allNodeInfo = gcsClient.getAllNodeInfo();
    Assert.assertEquals(allNodeInfo.size(), 1);
    Assert.assertEquals(allNodeInfo.get(0).nodeAddress, config.nodeIp);
    Assert.assertTrue(allNodeInfo.get(0).isAlive);
    Assert.assertEquals(allNodeInfo.get(0).resources.get("A"), 8.0);
  }

  @Test
  public void testNextJob() {
    RayConfig config = TestUtils.getRuntime().getRayConfig();
    // The value of job id of this driver in cluster should be 1.
    Assert.assertEquals(config.getJobId(), JobId.fromInt(1));

    GcsClient gcsClient = TestUtils.getRuntime().getGcsClient();
    for (int i = 2; i < 100; ++i) {
      Assert.assertEquals(gcsClient.nextJobId(), JobId.fromInt(i));
    }

  }
}
