package org.ray.streaming.runtime.graph;

import com.google.common.collect.Lists;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobGraphBuilder;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.ray.streaming.runtime.util.TestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GraphTest {

  private static final Logger LOG = LoggerFactory.getLogger(GraphTest.class);

  private JobMaster jobMaster;

  @org.testng.annotations.BeforeClass
  public void setUp() {
    TestHelper.setUTPattern();
    Map<String, String> jobConfig = new HashMap<>();
    jobMaster = new JobMaster(jobConfig);
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTPattern();
  }

  @org.testng.annotations.BeforeMethod
  public void testBegin(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " begin >>>>>>>>>>>>>>>>>>");
  }

  @org.testng.annotations.AfterMethod
  public void testEnd(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " end >>>>>>>>>>>>>>>>>>");
  }

  @Test
  public void testBuildExecutionGraph() {
    GraphManager graphManager = new GraphManagerImpl(jobMaster);
    JobGraph jobGraph = buildJobGraph();
    ExecutionGraph executionGraph = buildExecutionGraph(graphManager, jobGraph);
    List<ExecutionJobVertex> executionJobVertices = executionGraph.getExecutionJobVertexList();

    Assert.assertEquals(executionJobVertices.size(), jobGraph.getJobVertexList().size());

    int totalVertexNum = jobGraph.getJobVertexList().stream()
        .mapToInt(vertex -> vertex.getParallelism()).sum();
    Assert.assertEquals(executionGraph.getAllExecutionVertices().size(), totalVertexNum);

    int startIndex = 0;
    ExecutionJobVertex upStream = executionJobVertices.get(startIndex);
    ExecutionJobVertex downStream = executionJobVertices.get(startIndex + 1);
    Assert.assertEquals(upStream.getOutputEdges().get(0).getConsumer(), downStream);

    List<ExecutionVertex> upStreamVertices = upStream.getExecutionVertexList();
    List<ExecutionVertex> downStreamVertices = downStream.getExecutionVertexList();
    upStreamVertices.stream().forEach(vertex -> {
      vertex.getOutputEdges().stream().forEach(upStreamOutPutEdge -> {
        Assert.assertTrue(downStreamVertices.contains(upStreamOutPutEdge.getConsumer()));
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
    DataStream<String> dataStream = StreamSource.buildSource(streamingContext,
        Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOG.info(x));
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    JobGraph jobGraph = jobGraphBuilder.buildJobGraph();
    return jobGraph;
  }
}
