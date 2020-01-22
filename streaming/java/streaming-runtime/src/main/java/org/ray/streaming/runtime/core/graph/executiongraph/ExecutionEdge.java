package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;

/**
 * Edge to attach execution vertex.
 */
public class ExecutionEdge implements Serializable {

  /**
   * The source(upstream) execution vertex.
   */
  private final ExecutionVertex sourceVertex;

  /**
   * This target(downstream) execution vertex.
   */
  private final ExecutionVertex targetVertex;

  /**
   * A unique id for execution edge.
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

  public int getProducerId() {
    return sourceVertex.getVertexId();
  }

  public int getConsumerId() {
    return targetVertex.getVertexId();
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcVertex", sourceVertex)
        .add("targetVertex", targetVertex)
        .add("executionEdgeIndex", executionEdgeIndex)
        .toString();
  }

}
