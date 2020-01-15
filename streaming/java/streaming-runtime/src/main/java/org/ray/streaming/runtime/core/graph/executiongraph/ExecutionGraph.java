package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Physical plan.
 */
public class ExecutionGraph implements Serializable {

  private final String jobName;
  private List<ExecutionJobVertex> executionJobVertexList;
  private Map<Integer, ExecutionJobVertex> executionJobVertexMap;
  private Map<String, String> jobConfig;
  private int maxParallelism;
  private long buildTime;

  public ExecutionGraph(String jobName) {
    this.jobName = jobName;
    this.buildTime = System.currentTimeMillis();
  }

  public String getJobName() {
    return jobName;
  }

  public List<ExecutionJobVertex> getExecutionJobVertexList() {
    return executionJobVertexList;
  }

  public void setExecutionJobVertexList(
      List<ExecutionJobVertex> executionJobVertexList) {
    this.executionJobVertexList = executionJobVertexList;
  }

  public Map<Integer, ExecutionJobVertex> getExecutionJobVertexMap() {
    return executionJobVertexMap;
  }

  public void setExecutionJobVertexMap(
      Map<Integer, ExecutionJobVertex> executionJobVertexMap) {
    this.executionJobVertexMap = executionJobVertexMap;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public void setJobConfig(Map<String, String> jobConfig) {
    this.jobConfig = jobConfig;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public void setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public List<ExecutionVertex> getAllExecutionVertices() {
    return executionJobVertexList.stream()
        .map(ExecutionJobVertex::getExecutionVertexList)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  public List<ExecutionVertex> getAllAddedExecutionVertices() {
    return executionJobVertexList.stream()
        .map(ExecutionJobVertex::getExecutionVertexList)
        .flatMap(Collection::stream)
        .filter(vertex -> vertex.is2Add())
        .collect(Collectors.toList());
  }

  public ExecutionVertex getExecutionJobVertexByJobVertexId(int vertexId) {
    for (ExecutionJobVertex executionJobVertex : executionJobVertexList) {
      for (ExecutionVertex executionVertex : executionJobVertex.getExecutionVertexList()) {
        if (executionVertex.getVertexId() == vertexId) {
          return executionVertex;
        }
      }
    }
    throw new RuntimeException("Vertex " + vertexId + " does not exist!");
  }

}
