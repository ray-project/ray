package org.ray.streaming.runtime.resourcemanager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aeonbits.owner.util.Collections;
import org.ray.api.Ray;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.TestHelper;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManagerImpl;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.master.scheduler.strategy.impl.PipelineFirstStrategy;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.graph.ExecutionGraphTest;
import org.ray.streaming.runtime.master.JobRuntimeContext;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ResourceManagerTest extends BaseUnitTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerTest.class);

  @org.testng.annotations.BeforeClass
  public void setUp() {
    // ray init
    Ray.init();
    TestHelper.setUTFlag();
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTFlag();
  }

  @Test
  public void testApi() {
    Map<String, String> conf = new HashMap<String, String>();
    conf.put(CommonConfig.JOB_NAME, "testApi");
    conf.put(ResourceConfig.TASK_RESOURCE_CPU_LIMIT_ENABLE, "true");
    conf.put(ResourceConfig.TASK_RESOURCE_MEM_LIMIT_ENABLE, "true");
    conf.put(ResourceConfig.TASK_RESOURCE_MEM, "10");
    conf.put(ResourceConfig.TASK_RESOURCE_CPU, "2");
    StreamingConfig config = new StreamingConfig(conf);
    JobRuntimeContext jobRuntimeContext = new JobRuntimeContext(config);
    ResourceManager resourceManager = new ResourceManagerImpl(jobRuntimeContext);

    SlotAssignStrategy slotAssignStrategy = resourceManager.getSlotAssignStrategy();
    Assert.assertTrue(slotAssignStrategy instanceof PipelineFirstStrategy);

    Map<String, Double> containerResource = new HashMap<>();
    containerResource.put(ResourceConfig.RESOURCE_KEY_CPU, 16.0);
    containerResource.put(ResourceConfig.RESOURCE_KEY_MEM, 128.0);
    Container container1 = new Container(null, "testAddress1", "testHostName1");
    container1.setAvailableResource(containerResource);
    Container container2 = new Container(null, "testAddress2", "testHostName2");
    container2.setAvailableResource(new HashMap<>(containerResource));
    List<Container> containers = Collections.list(container1, container2);
    resourceManager.getResources().getRegisterContainers().addAll(containers);
    Assert.assertEquals(resourceManager.getRegisteredContainers().size(), 2);

    //build ExecutionGraph
    GraphManager graphManager = new GraphManagerImpl(new JobRuntimeContext(config));
    JobGraph jobGraph = ExecutionGraphTest.buildJobGraph();
    ExecutionGraph executionGraph = ExecutionGraphTest.buildExecutionGraph(graphManager, jobGraph);

    int slotNumPerContainer = slotAssignStrategy.getSlotNumPerContainer(containers, executionGraph
    .getMaxParallelism());
    Assert.assertEquals(slotNumPerContainer, 1);

    slotAssignStrategy.allocateSlot(containers, slotNumPerContainer);

    Map<ContainerID, List<Slot>> allocatingMap = slotAssignStrategy.assignSlot(executionGraph);
    Assert.assertEquals(allocatingMap.size(), 2);

    executionGraph.getAllAddedExecutionVertices().forEach(vertex -> {
      Container container = resourceManager.getResources()
          .getRegisterContainerByContainerId(vertex.getSlot().getContainerID());
      Map<String, Double> resource = resourceManager.allocateResource(container, vertex.getResources());
      Assert.assertNotNull(resource);
    });
    Assert.assertEquals(container1.getAvailableResource().get(ResourceConfig.RESOURCE_KEY_CPU), 14.0);
    Assert.assertEquals(container2.getAvailableResource().get(ResourceConfig.RESOURCE_KEY_CPU), 14.0);
    Assert.assertEquals(container1.getAvailableResource().get(ResourceConfig.RESOURCE_KEY_MEM), 118.0);
    Assert.assertEquals(container2.getAvailableResource().get(ResourceConfig.RESOURCE_KEY_MEM), 118.0);
  }
}
