package io.ray.streaming.runtime.core.graph;

import com.google.common.collect.Lists;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.api.stream.StreamSink;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobGraphBuilder;
import io.ray.streaming.jobgraph.JobVertex;
import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.config.master.ResourceConfig;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.ResourceType;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    GraphManager graphManager = new GraphManagerImpl(new JobMasterRuntimeContext(streamingConfig));
    JobGraph jobGraph = buildJobGraph();
    jobGraph.getJobConfig().put("streaming.task.resource.cpu.limitation.enable", "true");

    ExecutionGraph executionGraph = buildExecutionGraph(graphManager, jobGraph);
    List<ExecutionJobVertex> executionJobVertices = executionGraph.getExecutionJobVertexList();

    Assert.assertEquals(executionJobVertices.size(), jobGraph.getJobVertices().size());

    int totalVertexNum =
        jobGraph.getJobVertices().stream().mapToInt(JobVertex::getParallelism).sum();
    Assert.assertEquals(executionGraph.getAllExecutionVertices().size(), totalVertexNum);
    Assert.assertEquals(
        executionGraph.getAllExecutionVertices().size(),
        executionGraph.getExecutionVertexIdGenerator().get());

    executionGraph
        .getAllExecutionVertices()
        .forEach(
            vertex -> {
              Assert.assertNotNull(vertex.getStreamOperator());
              Assert.assertNotNull(vertex.getExecutionJobVertexName());
              Assert.assertNotNull(vertex.getVertexType());
              Assert.assertNotNull(vertex.getLanguage());
              Assert.assertEquals(
                  vertex.getExecutionVertexName(),
                  vertex.getExecutionJobVertexName() + "-" + vertex.getExecutionVertexIndex());
            });

    int startIndex = 0;
    ExecutionJobVertex upStream = executionJobVertices.get(startIndex);
    ExecutionJobVertex downStream = executionJobVertices.get(startIndex + 1);
    Assert.assertEquals(upStream.getOutputEdges().get(0).getTargetExecutionJobVertex(), downStream);

    List<ExecutionVertex> upStreamVertices = upStream.getExecutionVertices();
    List<ExecutionVertex> downStreamVertices = downStream.getExecutionVertices();
    upStreamVertices.forEach(
        vertex -> {
          Assert.assertEquals((double) vertex.getResource().get(ResourceType.CPU.name()), 2.0);
          vertex
              .getOutputEdges()
              .forEach(
                  upStreamOutPutEdge -> {
                    Assert.assertTrue(
                        downStreamVertices.contains(upStreamOutPutEdge.getTargetExecutionVertex()));
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
    DataStream<String> dataStream =
        DataStreamSource.fromCollection(streamingContext, Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOG.info(x));

    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put("key1", "value1");
    jobConfig.put("key2", "value2");
    jobConfig.put(ResourceConfig.TASK_RESOURCE_CPU, "2.0");
    jobConfig.put(ResourceConfig.TASK_RESOURCE_MEM, "2.0");

    JobGraphBuilder jobGraphBuilder =
        new JobGraphBuilder(Lists.newArrayList(streamSink), "test", jobConfig);

    return jobGraphBuilder.build();
  }
}
