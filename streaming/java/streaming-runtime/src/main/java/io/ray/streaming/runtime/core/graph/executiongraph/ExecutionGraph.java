package io.ray.streaming.runtime.core.graph.executiongraph;

import io.ray.api.RayActor;
import io.ray.streaming.runtime.worker.JobWorker;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

  /**
   * A monotonic increasing number, used for vertex's id(immutable).
   */
  private AtomicInteger executionVertexIdGenerator = new AtomicInteger(0);

  public ExecutionGraph(String jobName) {
    this.jobName = jobName;
    this.buildTime = System.currentTimeMillis();
  }

  public String getJobName() {
    return jobName;
  }

  public List<ExecutionJobVertex> getExecutionJobVertexList() {
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

  public int generateExecutionVertexId() {
    return executionVertexIdGenerator.getAndIncrement();
  }

  public AtomicInteger getExecutionVertexIdGenerator() {
    return executionVertexIdGenerator;
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
        if (executionVertex.getId() == vertexId) {
          return executionVertex;
        }
      }
    }
    throw new RuntimeException("Vertex " + vertexId + " does not exist!");
  }

  /**
   * Get all actors by graph.
   *
   * @return actor list
   */
  public List<RayActor<JobWorker>> getAllActors() {
    return getActorsFromJobVertices(getExecutionJobVertexList());
  }

  /**
   * Get source actors by graph.
   *
   * @return actor list
   */
  public List<RayActor<JobWorker>> getSourceActors() {
    List<ExecutionJobVertex> executionJobVertices = getExecutionJobVertexList().stream()
        .filter(ExecutionJobVertex::isSourceVertex)
        .collect(Collectors.toList());

    return getActorsFromJobVertices(executionJobVertices);
  }

  /**
   * Get transformation and sink actors by graph.
   *
   * @return actor list
   */
  public List<RayActor<JobWorker>> getNonSourceActors() {
    List<ExecutionJobVertex> executionJobVertices = getExecutionJobVertexList().stream()
        .filter(executionJobVertex -> executionJobVertex.isTransformationVertex()
            || executionJobVertex.isSinkVertex())
        .collect(Collectors.toList());

    return getActorsFromJobVertices(executionJobVertices);
  }

  /**
   * Get sink actors by graph.
   *
   * @return actor list
   */
  public List<RayActor<JobWorker>> getSinkActors() {
    List<ExecutionJobVertex> executionJobVertices = getExecutionJobVertexList().stream()
        .filter(ExecutionJobVertex::isSinkVertex)
        .collect(Collectors.toList());

    return getActorsFromJobVertices(executionJobVertices);
  }

  /**
   * Get actors according to job vertices.
   *
   * @param executionJobVertices specified job vertices
   * @return actor list
   */
  public List<RayActor<JobWorker>> getActorsFromJobVertices(
      List<ExecutionJobVertex> executionJobVertices) {
    return executionJobVertices.stream()
        .map(ExecutionJobVertex::getExecutionVertices)
        .flatMap(Collection::stream)
        .map(ExecutionVertex::getWorkerActor)
        .collect(Collectors.toList());
  }

}
