package org.ray.streaming.runtime.master.graphmanager;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobVertex;
import org.ray.streaming.runtime.core.graph.Graphs;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.master.JobRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphManagerImpl implements GraphManager {

  private static final Logger LOG = LoggerFactory.getLogger(GraphManagerImpl.class);

  protected final JobRuntimeContext runtimeContext;

  public GraphManagerImpl() {
    this.runtimeContext = null;
  }

  public GraphManagerImpl(JobRuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  @Override
  public ExecutionGraph buildExecutionGraph(JobGraph jobGraph) {
    LOG.info("Begin build execution graph with job graph {}.", jobGraph);

    // setup execution vertex
    ExecutionGraph executionGraph = setupExecutionVertex(jobGraph);

    // set max parallelism
    int maxParallelism = jobGraph.getJobVertexList().stream()
        .map(JobVertex::getParallelism)
        .max(Integer::compareTo).get();
    executionGraph.setMaxParallelism(maxParallelism);

    // set job config
    executionGraph.setJobConfig(jobGraph.getJobConfig());

    LOG.info("Build execution graph success.");
    return executionGraph;
  }

  @Override
  public ExecutionGraph setupExecutionVertex(JobGraph jobGraph) {
    ExecutionGraph executionGraph = new ExecutionGraph(jobGraph.getJobName());

    // create execution job vertex and execution vertex
    Map<Integer, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    long buildTime = executionGraph.getBuildTime();
    for (JobVertex jobVertex : jobGraph.getJobVertexList()) {
      int jobVertexId = jobVertex.getVertexId();
      exeJobVertexMap.put(jobVertexId, new ExecutionJobVertex(jobGraph.getJobName(), jobVertex,
          jobGraph.getJobConfig(), buildTime));
    }

    // attach vertex
    jobGraph.getJobEdgeList().stream().forEach(jobEdge -> {
      ExecutionJobVertex producer = exeJobVertexMap.get(jobEdge.getSrcVertexId());
      ExecutionJobVertex consumer = exeJobVertexMap.get(jobEdge.getTargetVertexId());

      ExecutionJobEdge executionJobEdge =
          new ExecutionJobEdge(producer, consumer, jobEdge);

      producer.getOutputEdges().add(executionJobEdge);
      consumer.getInputEdges().add(executionJobEdge);

      producer.getExecutionVertexList().stream().forEach(vertex -> {
        consumer.getExecutionVertexList().stream().forEach(outputVertex -> {
          ExecutionEdge executionEdge = new ExecutionEdge(vertex, outputVertex, executionJobEdge);
          vertex.getOutputEdges().add(executionEdge);
          outputVertex.getInputEdges().add(executionEdge);
        });
      });
    });

    // set execution job vertex into execution graph
    executionGraph.setExecutionJobVertexMap(exeJobVertexMap);

    return executionGraph;
  }

  @Override
  public Graphs getGraphs() {
    return runtimeContext.getGraphs();
  }

  @Override
  public JobGraph getJobGraph() {
    return getGraphs().getJobGraph();
  }

  @Override
  public ExecutionGraph getExecutionGraph() {
    return getGraphs().getExecutionGraph();
  }
}
