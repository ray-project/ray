package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
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
   * An unique id for execution edge.
   */
  private final String executionEdgeIndex;

  public ExecutionEdge(ExecutionVertex sourceVertex, ExecutionVertex targetVertex,
      ExecutionJobEdge executionJobEdge) {
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return sourceVertex.getVertexId() + "—" + targetVertex.getVertexId();
  }

  public ExecutionVertex getSourceVertex() {
    return sourceVertex;
  }

  public ExecutionVertex getTargetVertex() {
    return targetVertex;
  }

  public int getSourceVertexId() {
    return sourceVertex.getVertexId();
  }

  public int getTargetVertexId() {
    return targetVertex.getVertexId();
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcVertex", sourceVertex)
        .add("executionEdgeIndex", executionEdgeIndex)
        .toString();
  }

}
