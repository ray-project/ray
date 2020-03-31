package io.ray.streaming.runtime.graph;

import com.google.common.collect.Lists;


import java.util.HashMap;
import java.util.List;
import java.util.Map;


import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobGraphBuilder;
import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.master.JobRuntimeContext;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExecutionGraphTest extends BaseUnitTest {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphTest.class);

  @Test
  public void testBuildExecutionGraph() {
    Map<String, String> jobConf = new HashMap<>();
    StreamingConfig streamingConfig = new StreamingConfig(jobConf);
    GraphManager graphManager = new GraphManagerImpl(new JobRuntimeContext(streamingConfig));
    JobGraph jobGraph = buildJobGraph();
    ExecutionGraph executionGraph = buildExecutionGraph(graphManager, jobGraph);
    List<ExecutionJobVertex> executionJobVertices = executionGraph.getExecutionJobVertexLices();

    Assert.assertEquals(executionJobVertices.size(), jobGraph.getJobVertexList().size());

    int totalVertexNum = jobGraph.getJobVertexList().stream()
        .mapToInt(vertex -> vertex.getParallelism()).sum();
    Assert.assertEquals(executionGraph.getAllExecutionVertices().size(), totalVertexNum);

    int startIndex = 0;
    ExecutionJobVertex upStream = executionJobVertices.get(startIndex);
    ExecutionJobVertex downStream = executionJobVertices.get(startIndex + 1);
    Assert.assertEquals(upStream.getOutputEdges().get(0).getTargetVertex(), downStream);

    List<ExecutionVertex> upStreamVertices = upStream.getExecutionVertices();
    List<ExecutionVertex> downStreamVertices = downStream.getExecutionVertices();
    upStreamVertices.stream().forEach(vertex -> {
      vertex.getOutputEdges().stream().forEach(upStreamOutPutEdge -> {
        Assert.assertTrue(downStreamVertices.contains(upStreamOutPutEdge.getTargetVertex()));
      });
    });
  }

  public static ExecutionGraph buildExecutionGraph(GraphManager graphManager) {
    return graphManager.buildExecutionGraph(buildJobGraph());
  }

  public static ExecutionGraph buildExecutionGraph(GraphManager graphManager, JobGraph jobGraph) {
    return graphManager.buildExecutionGraph(jobGraph);
  }

  public static JobGraph buildJobGraph() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream = DataStreamSource.buildSource(streamingContext,
        Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOG.info(x));
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    JobGraph jobGraph = jobGraphBuilder.build();
    return jobGraph;
  }

}
