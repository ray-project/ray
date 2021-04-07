package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/** An edge that connects two execution vertices. */
public class ExecutionEdge implements Serializable {

  /** The source(upstream) execution vertex. */
  private final ExecutionVertex sourceExecutionVertex;

  /** The target(downstream) execution vertex. */
  private final ExecutionVertex targetExecutionVertex;

  /** The partition of current execution edge's execution job edge. */
  private final Partition partition;

  /** An unique id for execution edge. */
  private final String executionEdgeIndex;

  public ExecutionEdge(
      ExecutionVertex sourceExecutionVertex,
      ExecutionVertex targetExecutionVertex,
      ExecutionJobEdge executionJobEdge) {
    this.sourceExecutionVertex = sourceExecutionVertex;
    this.targetExecutionVertex = targetExecutionVertex;
    this.partition = executionJobEdge.getPartition();
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return sourceExecutionVertex.getExecutionVertexId()
        + "—"
        + targetExecutionVertex.getExecutionVertexId();
  }

  public ExecutionVertex getSourceExecutionVertex() {
    return sourceExecutionVertex;
  }

  public ExecutionVertex getTargetExecutionVertex() {
    return targetExecutionVertex;
  }

  public String getTargetExecutionJobVertexName() {
    return getTargetExecutionVertex().getExecutionJobVertexName();
  }

  public int getSourceVertexId() {
    return sourceExecutionVertex.getExecutionVertexId();
  }

  public int getTargetVertexId() {
    return targetExecutionVertex.getExecutionVertexId();
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
        .add("source", sourceExecutionVertex)
        .add("target", targetExecutionVertex)
        .add("partition", partition)
        .add("index", executionEdgeIndex)
        .toString();
  }
}
