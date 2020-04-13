package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/**
 * An edge that connects two execution vertices.
 */
public class ExecutionEdge implements Serializable {

  /**
   * The source(upstream) execution vertex.
   */
  private final ExecutionVertex sourceVertex;

  /**
   * The target(downstream) execution vertex.
   */
  private final ExecutionVertex targetVertex;

  /**
   * The partition of current execution edge's execution job edge.
   */
  private final Partition partition;

  /**
   * An unique id for execution edge.
   */
  private final String executionEdgeIndex;

  public ExecutionEdge(ExecutionVertex sourceVertex, ExecutionVertex targetVertex,
      ExecutionJobEdge executionJobEdge) {
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
    this.partition = executionJobEdge.getPartition();
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return sourceVertex.getId() + "—" + targetVertex.getId();
  }

  public ExecutionVertex getSourceVertex() {
    return sourceVertex;
  }

  public ExecutionVertex getTargetVertex() {
    return targetVertex;
  }

  public int getSourceVertexId() {
    return sourceVertex.getId();
  }

  public int getTargetVertexId() {
    return targetVertex.getId();
  }

  public Partition getPartition() {
    return partition;
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("sourceVertex", sourceVertex)
      .add("targetVertex", targetVertex)
      .add("partition", partition)
      .add("executionEdgeIndex", executionEdgeIndex)
      .toString();
  }
}
