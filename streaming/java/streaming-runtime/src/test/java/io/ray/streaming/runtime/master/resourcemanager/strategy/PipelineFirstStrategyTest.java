package io.ray.streaming.runtime.master.resourcemanager.strategy;

import io.ray.api.id.UniqueId;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.config.types.ResourceAssignStrategyType;
import io.ray.streaming.runtime.core.graph.ExecutionGraphTest;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.core.resource.ResourceType;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import io.ray.streaming.runtime.master.resourcemanager.ResourceAssignmentView;
import io.ray.streaming.runtime.master.resourcemanager.strategy.impl.PipelineFirstStrategy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class PipelineFirstStrategyTest extends BaseUnitTest {

  private Logger LOG = LoggerFactory.getLogger(PipelineFirstStrategyTest.class);

  private List<Container> containers = new ArrayList<>();
  private ResourceAssignStrategy strategy;

  @BeforeClass
  public void setUp() {
    strategy = new PipelineFirstStrategy();
    for (int i = 0; i < 2; ++i) {
      UniqueId uniqueId = UniqueId.randomId();
      Map<String, Double> resource = new HashMap<>();
      resource.put(ResourceType.CPU.getValue(), 4.0);
      resource.put(ResourceType.MEM.getValue(), 4.0);
      Container container = new Container("1.1.1." + i, uniqueId, "localhost" + i, resource);
      container.getAvailableResources().put(container.getName(), 500.0);
      containers.add(container);
    }
  }

  @AfterMethod
  public void tearDown() {
    reset();
  }

  private void reset() {
    containers = new ArrayList<>();
    strategy = null;
  }

  @Test
  public void testResourceAssignment() {
    strategy = new PipelineFirstStrategy();
    Assert.assertEquals(
        ResourceAssignStrategyType.PIPELINE_FIRST_STRATEGY.getName(), strategy.getName());

    Map<String, String> jobConf = new HashMap<>();
    StreamingConfig streamingConfig = new StreamingConfig(jobConf);
    GraphManager graphManager = new GraphManagerImpl(new JobMasterRuntimeContext(streamingConfig));
    JobGraph jobGraph = ExecutionGraphTest.buildJobGraph();
    ExecutionGraph executionGraph = ExecutionGraphTest.buildExecutionGraph(graphManager, jobGraph);
    ResourceAssignmentView assignmentView = strategy.assignResource(containers, executionGraph);
    Assert.assertNotNull(assignmentView);
  }
}
