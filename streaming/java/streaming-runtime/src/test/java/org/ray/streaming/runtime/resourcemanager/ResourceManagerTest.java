package org.ray.streaming.runtime.resourcemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.id.UniqueId;
import org.ray.api.runtimecontext.NodeInfo;
import org.ray.streaming.runtime.TestHelper;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.ray.streaming.runtime.core.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.core.master.resourcemanager.ResourceManagerImpl;
import org.ray.streaming.runtime.core.resource.Container;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ResourceManagerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerTest.class);

  private Object rayAsyncContext;

  @org.testng.annotations.BeforeClass
  public void setUp() {
    LOG.warn("Do set up");

    // ray init
    Ray.init();
    rayAsyncContext = Ray.getAsyncContext();

    TestHelper.setUTFlag();
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTFlag();
    LOG.warn("Do tear down");
  }

  @Test
  public void testApi() {
    Ray.setAsyncContext(rayAsyncContext);
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CommonConfig.JOB_NAME, "testApi");
    conf.put(QueueConfig.QUEUE_TYPE, QueueType.MEMORY_QUEUE.getName());
    ResourceManager resourceManager = new ResourceManagerImpl(
        new JobMaster(conf));
    List<Container> containers = resourceManager.getRegisteredContainers();
    assert containers.size() == 5;

    Container container = containers.get(0);

    assert resourceManager.allocateContainer(new ContainerSpec(), 1) == null;
    resourceManager.releaseContainer(Arrays.asList(container));

    Configuration configuration = new Configuration();
    JobVertex jobVertex = new JobVertex("1-jobvertex");
    jobVertex.setProcessor(new OneInputProcessor(null));
    ExecutionGraph graph = new ExecutionGraph();
    Map<String, String> jobConf = new HashMap<>();
    ExecutionJobVertex exeJobVertex = new ExecutionJobVertex(jobVertex,
        graph, jobConf);
    ExecutionVertex exeVertex = new ExecutionVertex("taskname", 0, 0,
        exeJobVertex);
    Map<String, Object> config = new HashMap<>();
    config.put("key", "value");
    exeVertex.withConfig(config);
    RayActor actor = resourceManager.allocateActor(container, LanguageType.JAVA,
        configuration, exeVertex);
    Assert.assertNotNull(actor);
    resourceManager.deallocateActor(actor);

    // TODO(qwang): Ray doesn't support `createPyActor()` in SINGLE_PROCESS mode.
//    actor = resourceManager.allocateActor(container, LanguageType.PYTHON,
//        configuration, exeVertex);
//    Assert.assertNotNull(actor);
  }

  @Test
  public void testUnregisterContainer() {
    Ray.setAsyncContext(rayAsyncContext);
    boolean autoScalingEnable = false;
    Map<String, String> conf = new HashMap();
    conf.put("streaming.scheduler.strategy.slot-assign", "colocate_strategy");
    conf.put(CommonConfig.JOB_NAME, "testUnregisterContainer");
    conf.put(QueueConfig.QUEUE_TYPE, QueueType.MEMORY_QUEUE.getName());
    ResourceManager resourceManager = new ResourceManagerImpl(
        new JobMaster(conf));
    ResourceManagerImpl spy = (ResourceManagerImpl) Mockito.spy(resourceManager);
    List<NodeInfo> nodeInfos = new ArrayList<>();

    String addr1 = "localhost1";
    String addr2 = "localhost2";
    String addr3 = "localhost3";
    UniqueId nodeId1 = createNodeId(1);
    UniqueId nodeId2 = createNodeId(2);
    UniqueId nodeId3 = createNodeId(3);

    NodeInfo nodeInfo1 = new NodeInfo(nodeId1, addr1, addr1, true, null, null);
    NodeInfo nodeInfo2 = new NodeInfo(nodeId2, addr2, addr2, true, null, null);
    nodeInfos.add(nodeInfo1);
    spy.checkAndUpdateResource();
    Assert.assertEquals(spy.getRegisteredContainers().size(), 5);
    nodeInfos.add(nodeInfo2);
    spy.checkAndUpdateResource();
    Assert.assertEquals(spy.getRegisteredContainers().size(), 5);
    nodeInfos.remove(nodeInfo1);
    nodeInfo1 = new NodeInfo(nodeId1, addr1, addr1, false,
        null, null);
    nodeInfos.add(nodeInfo1);
    spy.checkAndUpdateResource();
    Assert.assertEquals(spy.getRegisteredContainers().size(), 5);
    NodeInfo nodeInfo3 = new NodeInfo(nodeId3, addr3, addr3, true,
        null, null);
    nodeInfos.add(nodeInfo3);
    spy.checkAndUpdateResource();
    List<Container> containers = spy.getRegisteredContainers();
    Assert.assertEquals(containers.size(), 5);
    List<String> addrs = containers.stream().map(Container::getAddress)
        .collect(Collectors.toList());
    assert addrs.contains(addr2);
    assert addrs.contains(addr3);
  }

  @Test
  public void testJvmOption() {
    Ray.setAsyncContext(rayAsyncContext);
    boolean autoScalingEnable = false;
    Map<String, String> conf = new HashMap();
    conf.put(CommonConfig.JOB_NAME, "testUnregisterContainer");
    conf.put(QueueConfig.QUEUE_TYPE, QueueType.MEMORY_QUEUE.getName());
    ResourceManager resourceManager = new ResourceManagerImpl(
        new JobMaster(conf));
    List<Container> containers = resourceManager.getRegisteredContainers();
    assert containers.size() == 5;

    Container container = containers.get(0);

    assert resourceManager.allocateContainer(new ContainerSpec(), 1) == null;
    resourceManager.releaseContainer(Arrays.asList(container));

    JobVertex jobVertex = new JobVertex("1-jobvertex");
    jobVertex.setProcessor(new OneInputProcessor(null));
    ExecutionGraph graph = new ExecutionGraph();
    Map<String, String> jobConf = new HashMap<>();
    ExecutionJobVertex exeJobVertex = new ExecutionJobVertex(jobVertex, graph, jobConf);
    Map<String, Double> resource = new HashMap<>();
    resource.put("Xms", 1024.0);
    resource.put("Xmx", 1024.0);
    exeJobVertex.getJobVertex().setResources(resource);
    ExecutionVertex exeVertex = new ExecutionVertex("taskname", 0, 0,
        exeJobVertex);
    Map<String, Object> config = new HashMap<>();
    config.put("JVM_OPTIONS", " -Xms=1g -Xmx=1g ");
    exeVertex.withConfig(config);
    RayActor actor = resourceManager.allocateActor(container, LanguageType.JAVA,
        exeVertex.getExecutionConfig().getConfiguration(), exeVertex);
    Assert.assertNotNull(actor);
    resourceManager.deallocateActor(actor);
  }

  private UniqueId createNodeId(int id) {
    byte[] nodeIdBytes = new byte[UniqueId.LENGTH];
    for (int byteIndex = 0; byteIndex < UniqueId.LENGTH; ++byteIndex) {
      nodeIdBytes[byteIndex] = String.valueOf(id).getBytes()[0];
    }
    return new UniqueId(nodeIdBytes);
  }