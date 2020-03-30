package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.aeonbits.owner.ConfigFactory;
import org.ray.api.RayActor;
import org.ray.streaming.jobgraph.JobVertex;
import org.ray.streaming.jobgraph.VertexType;
import org.ray.streaming.operator.StreamOperator;
import org.ray.streaming.runtime.config.master.ResourceConfig;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Physical job vertex.
 *
 * <p>Execution job vertex is the physical form of {@link JobVertex} and
 * every execution job vertex is corresponding to a group of {@link ExecutionVertex}.
 */
public class ExecutionJobVertex {

  /**
   * Unique id of operator(use {@link JobVertex}'s id).
   */
  private final int operatorId;

  /**
   * Unique name of operator(use {@link StreamOperator}'s name).
   */
  private final String operatorName;
  private final StreamOperator streamOperator;
  private final VertexType vertexType;
  private final Map<String, String> jobConfig;

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
      JobVertex jobVertex, Map<String, String> jobConfig, AtomicInteger globalIndex) {
    this.operatorId = jobVertex.getVertexId();
    this.operatorName = jobVertex.getStreamOperator().getName();
    this.streamOperator = jobVertex.getStreamOperator();
    this.vertexType = jobVertex.getVertexType();
    this.jobConfig = jobConfig;
    this.parallelism = jobVertex.getParallelism();
    this.executionVertices = createExecutionVertics(globalIndex);
  }

  private List<ExecutionVertex> createExecutionVertics(AtomicInteger gloabalIndex) {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    ResourceConfig resourceConfig = ConfigFactory.create(ResourceConfig.class, jobConfig);

    for (int subIndex = 0; subIndex < parallelism; subIndex++) {
      executionVertices.add(new ExecutionVertex(
        gloabalIndex.getAndIncrement(), subIndex, this, resourceConfig));
    }
    return executionVertices;
  }

  public Map<Integer, RayActor<JobWorker>> getExecutionVertexWorkers() {
    Map<Integer, RayActor<JobWorker>> executionVertexWorkersMap = new HashMap<>();

    Preconditions.checkArgument(
        executionVertices != null && !executionVertices.isEmpty(),
        "Empty execution vertex.");
    executionVertices.stream().forEach(vertex -> {
      Preconditions.checkArgument(
          vertex.getWorkerActor() != null,
          "Empty execution vertex worker actor.");
      executionVertexWorkersMap.put(vertex.getId(), vertex.getWorkerActor());
    });

    return executionVertexWorkersMap;
  }

  public int getOperatorId() {
    return operatorId;
  }

  public String getOperatorName() {
    return operatorName;
  }

  /**
   * e.g. 1-SourceOperator
   *
   * @return operator name with index
   */
  public String getVertexNameWithIndex() {
    return operatorId + "-" + operatorName;
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
        .add("jobVertexId", operatorId)
        .add("jobVertexName", operatorName)
        .add("vertexType", vertexType)
        .add("parallelism", parallelism)
        .toString();
  }
}
