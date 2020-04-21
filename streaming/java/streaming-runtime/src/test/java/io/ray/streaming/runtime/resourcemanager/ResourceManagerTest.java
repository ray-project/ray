package io.ray.streaming.runtime.resourcemanager;

import java.util.Map;

import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.TestHelper;
import io.ray.api.Ray;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.runtime.util.Mockitools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ResourceManagerTest extends BaseUnitTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerTest.class);

  private Object rayAsyncContext;

  @org.testng.annotations.BeforeClass
  public void setUp() {
    LOG.warn("Do set up");

    TestHelper.setUTFlag();
    // ray init
    Ray.init();
    rayAsyncContext = Ray.getAsyncContext();
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    LOG.warn("Do tear down");
    TestHelper.clearUTFlag();
  }

  @Test
  public void testGcsMockedApi() {
    Map<UniqueId, NodeInfo> nodeInfoMap = Mockitools.mockGetNodeInfoMap(Mockitools.mockGetAllNodeInfo());
    Assert.assertEquals(nodeInfoMap.size(), 5);
  }
}
