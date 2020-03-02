package org.ray.streaming.runtime.schedule;

import com.google.common.collect.Lists;
import java.util.List;
import org.ray.api.Ray;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.partition.impl.RoundRobinPartition;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.DataStreamSink;
import org.ray.streaming.api.stream.DataStreamSource;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobGraphBuilder;
import org.ray.streaming.runtime.BaseUnitTest;
import org.ray.streaming.runtime.core.graph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionNode.NodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TaskAssignerImplTest extends BaseUnitTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskAssignerImplTest.class);

  @Test
  public void testTaskAssignImpl() {
    Ray.init();
    JobGraph jobGraph = buildDataSyncPlan();

    TaskAssigner taskAssigner = new TaskAssignerImpl();
    ExecutionGraph executionGraph = taskAssigner.assign(jobGraph);

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

    Ray.shutdown();
  }

  public JobGraph buildDataSyncPlan() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream = DataStreamSource.buildSource(streamingContext,
        Lists.newArrayList("a", "b", "c"));
    DataStreamSink streamSink = dataStream.sink(LOGGER::info);
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    return jobGraphBuilder.build();
  }
}
