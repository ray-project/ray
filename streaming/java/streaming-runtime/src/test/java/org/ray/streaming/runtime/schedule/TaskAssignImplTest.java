package org.ray.streaming.runtime.schedule;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.api.id.ObjectId;
import org.ray.runtime.actor.LocalModeRayActor;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.partition.impl.RoundRobinPartition;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.runtime.core.graph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionNode.NodeType;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.plan.Plan;
import org.ray.streaming.plan.PlanBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TaskAssignImplTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskAssignImplTest.class);

  @Test
  public void testTaskAssignImpl() {
    Plan plan = buildDataSyncPlan();

    List<RayActor<JobWorker>> workers = new ArrayList<>();
    for(int i = 0; i < plan.getPlanVertexList().size(); i++) {
      workers.add(new LocalModeRayActor(ActorId.fromRandom(), ObjectId.fromRandom()));
    }

    ITaskAssign taskAssign = new TaskAssignImpl();
    ExecutionGraph executionGraph = taskAssign.assign(plan, workers);

    List<ExecutionNode> executionNodeList = executionGraph.getExecutionNodeList();

    Assert.assertEquals(executionNodeList.size(), 2);
    ExecutionNode sourceNode = executionNodeList.get(0);
    Assert.assertEquals(sourceNode.getNodeType(), NodeType.SOURCE);
    Assert.assertEquals(sourceNode.getExecutionTasks().size(), 1);
    Assert.assertEquals(sourceNode.getOutputEdges().size(), 1);

    List<ExecutionEdge> sourceExecutionEdges = sourceNode.getOutputEdges();

    Assert.assertEquals(sourceExecutionEdges.size(), 1);
    ExecutionEdge source2Sink = sourceExecutionEdges.get(0);

    Assert.assertEquals(source2Sink.getPartition().getClass(), RoundRobinPartition.class);

    ExecutionNode sinkNode = executionNodeList.get(1);
    Assert.assertEquals(sinkNode.getNodeType(), NodeType.SINK);
    Assert.assertEquals(sinkNode.getExecutionTasks().size(), 1);
    Assert.assertEquals(sinkNode.getOutputEdges().size(), 0);
  }

  public Plan buildDataSyncPlan() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream = StreamSource.buildSource(streamingContext,
        Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOGGER.info(x));
    PlanBuilder planBuilder = new PlanBuilder(Lists.newArrayList(streamSink));

    Plan plan = planBuilder.buildPlan();
    return plan;
  }
}
