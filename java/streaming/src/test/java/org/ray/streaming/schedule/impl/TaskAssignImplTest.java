package org.ray.streaming.schedule.impl;

import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.runtime.actor.LocalModeRayActor;
import org.ray.streaming.api.partition.impl.RoundRobinPartition;
import org.ray.streaming.core.graph.ExecutionEdge;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.graph.ExecutionNode;
import org.ray.streaming.core.graph.ExecutionNode.NodeType;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanBuilderTest;
import org.ray.streaming.schedule.ITaskAssign;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.RayActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TaskAssignImplTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskAssignImplTest.class);

  @Test
  public void testTaskAssignImpl() {
    PlanBuilderTest planBuilderTest = new PlanBuilderTest();
    Plan plan = planBuilderTest.buildDataSyncPlan();

    List<RayActor<StreamWorker>> workers = new ArrayList<>();
    for(int i = 0; i < plan.getPlanVertexList().size(); i++) {
      workers.add(new LocalModeRayActor(ActorId.fromRandom(), ObjectId.fromRandom()));
    }

    ITaskAssign taskAssign = new TaskAssignImpl();
    ExecutionGraph executionGraph = taskAssign.assign(plan, workers);

    List<ExecutionNode> executionNodeList = executionGraph.getExecutionNodeList();

    Assert.assertEquals(executionNodeList.size(), 2);
    ExecutionNode sourceNode = executionNodeList.get(0);
    Assert.assertEquals(sourceNode.getNodeType(), NodeType.SOURCE);
    Assert.assertEquals(sourceNode.getExecutionTaskList().size(), 1);
    Assert.assertEquals(sourceNode.getExecutionEdgeList().size(), 1);

    List<ExecutionEdge> sourceExecutionEdges = sourceNode.getExecutionEdgeList();

    Assert.assertEquals(sourceExecutionEdges.size(), 1);
    ExecutionEdge source2Sink = sourceExecutionEdges.get(0);

    Assert.assertEquals(source2Sink.getPartition().getClass(), RoundRobinPartition.class);

    ExecutionNode sinkNode = executionNodeList.get(1);
    Assert.assertEquals(sinkNode.getNodeType(), NodeType.SINK);
    Assert.assertEquals(sinkNode.getExecutionTaskList().size(), 1);
    Assert.assertEquals(sinkNode.getExecutionEdgeList().size(), 0);
  }
}
