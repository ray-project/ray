package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.streaming.api.Language;
import io.ray.streaming.jobgraph.JobVertex;
import io.ray.streaming.jobgraph.VertexType;
import io.ray.streaming.operator.StreamOperator;
import io.ray.streaming.runtime.config.master.ResourceConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.aeonbits.owner.ConfigFactory;

/**
 * Physical job vertex.
 *
 * <p>Execution job vertex is the physical form of {@link JobVertex} and
 * every execution job vertex is corresponding to a group of {@link ExecutionVertex}.
 */
public class ExecutionJobVertex {

  /**
   * Unique id. Use {@link JobVertex}'s id directly.
   */
  private final int executionJobVertexId;

  /**
   * Use jobVertex id and operator(use {@link StreamOperator}'s name) as name.
   * e.g. 1-SourceOperator
   */
  private final String executionJobVertexName;
  private final StreamOperator streamOperator;
  private final VertexType vertexType;
  private final Language language;
  private final Map<String, String> jobConfig;
  private final long buildTime;

  /**
   * Parallelism of current execution job vertex(operator).
   */
  private int parallelism;

  /**
   * Sub execution vertices of current execution job vertex(operator).
   */
  private List<ExecutionVertex> executionVertices;

  /**
   * Input and output edges of current execution job vertex.
   */
  private List<ExecutionJobEdge> inputEdges = new ArrayList<>();
  private List<ExecutionJobEdge> outputEdges = new ArrayList<>();

  public ExecutionJobVertex(
      JobVertex jobVertex,
      Map<String, String> jobConfig,
      AtomicInteger idGenerator,
      long buildTime) {
    this.executionJobVertexId = jobVertex.getVertexId();
    this.executionJobVertexName = generateExecutionJobVertexName(
        executionJobVertexId, jobVertex.getStreamOperator().getName());
    this.streamOperator = jobVertex.getStreamOperator();
    this.vertexType = jobVertex.getVertexType();
    this.language = jobVertex.getLanguage();
    this.jobConfig = jobConfig;
    this.buildTime = buildTime;
    this.parallelism = jobVertex.getParallelism();
    this.executionVertices = createExecutionVertices(idGenerator);
  }

  private List<ExecutionVertex> createExecutionVertices(AtomicInteger idGenerator) {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    ResourceConfig resourceConfig = ConfigFactory.create(ResourceConfig.class, jobConfig);

    for (int subIndex = 0; subIndex < parallelism; subIndex++) {
      executionVertices.add(new ExecutionVertex(
          idGenerator.getAndIncrement(), subIndex, this, resourceConfig));
    }
    return executionVertices;
  }

  private String generateExecutionJobVertexName(int jobVertexId, String streamOperatorName) {
    return jobVertexId + "-" + streamOperatorName;
  }

  public Map<Integer, BaseActorHandle> getExecutionVertexWorkers() {
    Map<Integer, BaseActorHandle> executionVertexWorkersMap = new HashMap<>();

    Preconditions.checkArgument(
        executionVertices != null && !executionVertices.isEmpty(),
        "Empty execution vertex.");
    executionVertices.stream().forEach(vertex -> {
      Preconditions.checkArgument(
          vertex.getWorkerActor() != null,
          "Empty execution vertex worker actor.");
      executionVertexWorkersMap.put(vertex.getExecutionVertexId(), vertex.getWorkerActor());
    });

    return executionVertexWorkersMap;
  }

  public int getExecutionJobVertexId() {
    return executionJobVertexId;
  }

  public String getExecutionJobVertexName() {
    return executionJobVertexName;
  }

  /**
   * e.g. 1-SourceOperator
   *
   * @return operator name with index
   */
  public String getExecutionJobVertexNameWithIndex() {
    return executionJobVertexId + "-" + executionJobVertexName;
  }

  public int getParallelism() {
    return parallelism;
  }

  public List<ExecutionVertex> getExecutionVertices() {
    return executionVertices;
  }

  public void setExecutionVertices(
      List<ExecutionVertex> executionVertex) {
    this.executionVertices = executionVertex;
  }

  public List<ExecutionJobEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(
      List<ExecutionJobEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public List<ExecutionJobEdge> getInputEdges() {
    return inputEdges;
  }

  public void setInputEdges(
      List<ExecutionJobEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public Language getLanguage() {
    return language;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public long getBuildTime() {
    return buildTime;
  }

  public boolean isSourceVertex() {
    return getVertexType() == VertexType.SOURCE;
  }

  public boolean isTransformationVertex() {
    return getVertexType() == VertexType.TRANSFORMATION;
  }

  public boolean isSinkVertex() {
    return getVertexType() == VertexType.SINK;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("executionJobVertexId", executionJobVertexId)
        .add("executionJobVertexName", executionJobVertexName)
        .add("vertexType", vertexType)
        .add("parallelism", parallelism)
        .toString();
  }
}
