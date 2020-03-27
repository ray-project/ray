package org.ray.streaming.runtime.schedule.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import org.aeonbits.owner.ConfigFactory;
import org.ray.api.id.UniqueId;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.master.scheduler.strategy.impl.PipelineFirstStrategy;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.core.resource.ContainerID;
import org.ray.streaming.runtime.core.resource.Resources;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.graph.ExecutionGraphTest;
import org.ray.streaming.runtime.master.JobRuntimeContext;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PipelineFirstStrategyTest extends BaseUnitTest {

  private Logger LOG = LoggerFactory.getLogger(PipelineFirstStrategyTest.class);

  private SlotAssignStrategy strategy;
  private List<Container> containers = new ArrayList<>();
  private JobGraph jobGraph;
  private ExecutionGraph executionGraph;
  private int maxParallelism;

  @BeforeClass
  public void setUp() {
    strategy = new PipelineFirstStrategy();
    Map<String, String> conf = new HashMap<>();
    ResourceConfig resourceConfig = ConfigFactory.create(ResourceConfig.class, conf);
    Resources resources = new Resources(resourceConfig);

    Map<String, Double> containerResource = new HashMap<>();
    containerResource.put(ResourceConfig.RESOURCE_KEY_CPU, 16.0);
    containerResource.put(ResourceConfig.RESOURCE_KEY_MEM, 128.0);
    for (int i = 0; i < 2; ++i) {
      UniqueId uniqueId = UniqueId.randomId();
      Container container = new Container(uniqueId, "1.1.1." + i,  "localhost" + i);
      container.setAvailableResource(containerResource);
      containers.add(container);
      resources.getRegisterContainers().add(container);
    }
    strategy.setResources(resources);

    //build ExecutionGraph
    Map<String, String> jobConf = new HashMap<>();
    StreamingConfig streamingConfig = new StreamingConfig(jobConf);
    GraphManager graphManager = new GraphManagerImpl(new JobRuntimeContext(streamingConfig));
    jobGraph = ExecutionGraphTest.buildJobGraph();
    executionGraph = ExecutionGraphTest.buildExecutionGraph(graphManager, jobGraph);
    maxParallelism = executionGraph.getMaxParallelism();
  }

  @Test
  public int testSlotNumPerContainer() {
    int slotNumPerContainer = strategy.getSlotNumPerContainer(containers, maxParallelism);
    Assert.assertEquals(slotNumPerContainer,
        (int) Math.ceil(Math.max(maxParallelism, containers.size()) * 1.0 / containers.size()));
    return slotNumPerContainer;
  }

  @Test
  public void testAllocateSlot() {
    int slotNumPerContainer = testSlotNumPerContainer();
    strategy.allocateSlot(containers, slotNumPerContainer);
    for (Container container : containers) {
      Assert.assertEquals(container.getSlots().size(), slotNumPerContainer);
    }
  }

  @Test
  public void testAssignSlot() {
    Map<ContainerID, List<Slot>> allocatingMap = strategy.assignSlot(executionGraph);
    for (Entry<ContainerID, List<Slot>> containerSlotEntry : allocatingMap.entrySet()) {
      containerSlotEntry.getValue()
          .forEach(slot -> Assert.assertNotEquals(slot.getExecutionVertexIds().size(), 0));
    }
  }
}
