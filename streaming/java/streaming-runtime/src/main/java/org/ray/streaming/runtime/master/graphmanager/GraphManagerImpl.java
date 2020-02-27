package org.ray.streaming.runtime.master.graphmanager;

import java.util.LinkedHashMap;
import java.util.Map;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobVertex;
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

  public GraphManagerImpl(JobRuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  @Override
  public ExecutionGraph buildExecutionGraph(JobGraph jobGraph) {
    LOG.info("Begin build execution graph with job graph {}.", jobGraph);

    // setup structure
    ExecutionGraph executionGraph = setupStructure(jobGraph);

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

  private ExecutionGraph setupStructure(JobGraph jobGraph) {
    ExecutionGraph executionGraph = new ExecutionGraph(jobGraph.getJobName());

    // create vertex
    Map<Integer, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    long buildTime = executionGraph.getBuildTime();
    for (JobVertex jobVertex : jobGraph.getJobVertexList()) {
      int jobVertexId = jobVertex.getVertexId();
      exeJobVertexMap.put(jobVertexId,
          new ExecutionJobVertex(jobVertex, runtimeContext));
    }

    // connect vertex
    jobGraph.getJobEdgeList().stream().forEach(jobEdge -> {
      ExecutionJobVertex source = exeJobVertexMap.get(jobEdge.getSrcVertexId());
      ExecutionJobVertex target = exeJobVertexMap.get(jobEdge.getTargetVertexId());

      ExecutionJobEdge executionJobEdge =
          new ExecutionJobEdge(source, target, jobEdge);

      source.getOutputEdges().add(executionJobEdge);
      target.getInputEdges().add(executionJobEdge);

      source.getExecutionVertices().stream().forEach(vertex -> {
        target.getExecutionVertices().stream().forEach(outputVertex -> {
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
  public JobGraph getJobGraph() {
    return runtimeContext.getJobGraph();
  }

  @Override
  public ExecutionGraph getExecutionGraph() {
    return runtimeContext.getExecutionGraph();
  }

}
