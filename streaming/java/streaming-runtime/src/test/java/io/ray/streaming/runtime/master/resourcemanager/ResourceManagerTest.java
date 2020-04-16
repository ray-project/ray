package io.ray.streaming.runtime.master.resourcemanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import io.ray.api.Ray;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.NodeInfo;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.config.global.CommonConfig;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.JobRuntimeContext;
import io.ray.streaming.runtime.util.Mockitools;
import io.ray.streaming.runtime.util.RayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.IObjectFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

@PrepareForTest(RayUtils.class)
@PowerMockIgnore({"org.slf4j.*", "javax.xml.*"})
public class ResourceManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerTest.class);

  private Object rayAsyncContext;

  @ObjectFactory
  public IObjectFactory getObjectFactory() {
    return new org.powermock.modules.testng.PowerMockObjectFactory();
  }

  @org.testng.annotations.BeforeClass
  public void setUp() {
    LOG.warn("Do set up");
    MockitoAnnotations.initMocks(this);
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    LOG.warn("Do tear down");
  }

  @BeforeMethod
  public void mockGscApi() {
    // ray init
    Ray.init();
    rayAsyncContext = Ray.getAsyncContext();
    Mockitools.mockGscApi();
  }

  @Test
  public void testGcsMockedApi() {
    Map<UniqueId, NodeInfo> nodeInfoMap = RayUtils.getAliveNodeInfoMap();
    Assert.assertEquals(nodeInfoMap.size(), 5);
  }

  @Test
  public void testApi() {
    Ray.setAsyncContext(rayAsyncContext);

    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CommonConfig.JOB_NAME, "testApi");
    StreamingConfig config = new StreamingConfig(conf);
    JobRuntimeContext jobRuntimeContext = new JobRuntimeContext(config);
    ResourceManager resourceManager = new ResourceManagerImpl(jobRuntimeContext);

    // test register container
    List<Container> containers = resourceManager.getRegisteredContainers();
    Assert.assertEquals(containers.size(), 5);
  }
}
