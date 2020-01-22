package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Physical plan.
 */
public class ExecutionGraph implements Serializable {

  /**
   * Name of the job.
   */
  private final String jobName;

  /**
   * Configuration of the job.
   */
  private Map<String, String> jobConfig;

  /**
   * Data map for execution job vertex.
   * key: job vertex id.
   * value: execution job vertex.
   */
  private Map<Integer, ExecutionJobVertex> executionJobVertexMap;

  /**
   * The max parallelism of the whole graph.
   */
  private int maxParallelism;

  /**
   * Build time.
   */
  private long buildTime;

  public ExecutionGraph(String jobName) {
    this.jobName = jobName;
    this.buildTime = System.currentTimeMillis();
  }

  public String getJobName() {
    return jobName;
  }

  public List<ExecutionJobVertex> getExecutionJobVertexLices() {
    return new ArrayList<ExecutionJobVertex>(executionJobVertexMap.values());
  }

  public Map<Integer, ExecutionJobVertex> getExecutionJobVertexMap() {
    return executionJobVertexMap;
  }

  public void setExecutionJobVertexMap(Map<Integer, ExecutionJobVertex> executionJobVertexMap) {
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

  /**
   * Get all execution vertices from current execution graph.
   *
   * @return all execution vertices.
   */
  public List<ExecutionVertex> getAllExecutionVertices() {
    return executionJobVertexMap.values().stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Get all execution vertices whose status is 'TO_ADD' from current execution graph.
   *
   * @return all added execution vertices.
   */
  public List<ExecutionVertex> getAllAddedExecutionVertices() {
    return executionJobVertexMap.values().stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .filter(vertex -> vertex.is2Add())
        .collect(Collectors.toList());
  }

  /**
   * Get specified execution vertex from current execution graph by execution vertex id.
   *
   * @param vertexId execution vertex id.
   * @return the specified execution vertex.
   */
  public ExecutionVertex getExecutionJobVertexByJobVertexId(int vertexId) {
    for (ExecutionJobVertex executionJobVertex : executionJobVertexMap.values()) {
      for (ExecutionVertex executionVertex : executionJobVertex.getExecutionVertices()) {
        if (executionVertex.getVertexId() == vertexId) {
          return executionVertex;
        }
      }
    }
    throw new RuntimeException("Vertex " + vertexId + " does not exist!");
  }

}
