package io.ray.streaming.runtime.master.resourcemanager;

import io.ray.api.Ray;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.config.global.CommonConfig;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import io.ray.streaming.runtime.util.RayUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ResourceManagerTest extends BaseUnitTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerTest.class);

  private Object rayAsyncContext;

  @BeforeMethod
  public void init() {
    // ray init
    Ray.init();
    rayAsyncContext = Ray.getAsyncContext();
  }

  @Test
  public void testGcsMockedApi() {
    Map<UniqueId, NodeInfo> nodeInfoMap = RayUtils.getAliveNodeInfoMap();
    Assert.assertEquals(nodeInfoMap.size(), 5);
  }

  @Test(dependsOnMethods = "testGcsMockedApi")
  public void testApi() {
    Ray.setAsyncContext(rayAsyncContext);

    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CommonConfig.JOB_NAME, "testApi");
    StreamingConfig config = new StreamingConfig(conf);
    JobMasterRuntimeContext jobMasterRuntimeContext = new JobMasterRuntimeContext(config);
    ResourceManager resourceManager = new ResourceManagerImpl(jobMasterRuntimeContext);

    // test register container
    List<Container> containers = resourceManager.getRegisteredContainers();
    Assert.assertEquals(containers.size(), 5);
  }
}
