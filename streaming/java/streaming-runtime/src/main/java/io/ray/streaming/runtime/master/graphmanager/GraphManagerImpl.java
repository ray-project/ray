package io.ray.streaming.runtime.master.graphmanager;

import io.ray.api.BaseActorHandle;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.jobgraph.JobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphManagerImpl implements GraphManager {

  private static final Logger LOG = LoggerFactory.getLogger(GraphManagerImpl.class);

  protected final JobMasterRuntimeContext runtimeContext;

  public GraphManagerImpl(JobMasterRuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  @Override
  public ExecutionGraph buildExecutionGraph(JobGraph jobGraph) {
    LOG.info("Begin build execution graph with job graph {}.", jobGraph);

    // setup structure
    ExecutionGraph executionGraph = setupStructure(jobGraph);

    // set max parallelism
    int maxParallelism =
        jobGraph.getJobVertices().stream()
            .map(JobVertex::getParallelism)
            .max(Integer::compareTo)
            .get();
    executionGraph.setMaxParallelism(maxParallelism);

    // set job config
    executionGraph.setJobConfig(jobGraph.getJobConfig());

    LOG.info("Build execution graph success.");
    return executionGraph;
  }

  private ExecutionGraph setupStructure(JobGraph jobGraph) {
    ExecutionGraph executionGraph = new ExecutionGraph(jobGraph.getJobName());
    Map<String, String> jobConfig = jobGraph.getJobConfig();

    // create vertex
    Map<Integer, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    Map<Integer, ExecutionVertex> executionVertexMap = new HashMap<>();
    long buildTime = executionGraph.getBuildTime();
    for (JobVertex jobVertex : jobGraph.getJobVertices()) {
      int jobVertexId = jobVertex.getVertexId();
      exeJobVertexMap.put(
          jobVertexId,
          new ExecutionJobVertex(
              jobVertex, jobConfig, executionGraph.getExecutionVertexIdGenerator(), buildTime));
    }

    // for each job edge, connect all source exeVertices and target exeVertices
    jobGraph
        .getJobEdges()
        .forEach(
            jobEdge -> {
              ExecutionJobVertex source = exeJobVertexMap.get(jobEdge.getSrcVertexId());
              ExecutionJobVertex target = exeJobVertexMap.get(jobEdge.getTargetVertexId());

              ExecutionJobEdge executionJobEdge = new ExecutionJobEdge(source, target, jobEdge);

              source.getOutputEdges().add(executionJobEdge);
              target.getInputEdges().add(executionJobEdge);

              source
                  .getExecutionVertices()
                  .forEach(
                      sourceExeVertex -> {
                        target
                            .getExecutionVertices()
                            .forEach(
                                targetExeVertex -> {
                                  // pre-process some mappings
                                  executionVertexMap.put(
                                      targetExeVertex.getExecutionVertexId(), targetExeVertex);
                                  executionVertexMap.put(
                                      sourceExeVertex.getExecutionVertexId(), sourceExeVertex);
                                  // build execution edge
                                  ExecutionEdge executionEdge =
                                      new ExecutionEdge(
                                          sourceExeVertex, targetExeVertex, executionJobEdge);
                                  sourceExeVertex.getOutputEdges().add(executionEdge);
                                  targetExeVertex.getInputEdges().add(executionEdge);
                                });
                      });
            });

    // set execution job vertex into execution graph
    executionGraph.setExecutionJobVertexMap(exeJobVertexMap);
    executionGraph.setExecutionVertexMap(executionVertexMap);

    return executionGraph;
  }

  private void addActorToChannelGroupedActors(
      Map<String, Set<BaseActorHandle>> channelGroupedActors,
      String channelId,
      BaseActorHandle actor) {

    Set<BaseActorHandle> actorSet =
        channelGroupedActors.computeIfAbsent(channelId, k -> new HashSet<>());
    actorSet.add(actor);
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
