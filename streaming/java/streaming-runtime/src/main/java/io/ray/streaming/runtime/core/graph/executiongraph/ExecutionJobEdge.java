package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.jobgraph.JobEdge;

/**
 * An edge that connects two execution job vertices.
 */
public class ExecutionJobEdge {

  /**
   * The source(upstream) execution job vertex.
   */
  private final ExecutionJobVertex sourceVertex;

  /**
   * The target(downstream) execution job vertex.
   */
  private final ExecutionJobVertex targetVertex;

  /**
   * The partition of the execution job edge.
   */
  private final Partition partition;

  /**
   * An unique id for execution job edge.
   */
  private final String executionJobEdgeIndex;

  public ExecutionJobEdge(ExecutionJobVertex sourceVertex, ExecutionJobVertex targetVertex,
      JobEdge jobEdge) {
    this.sourceVertex = sourceVertex;
    this.targetVertex = targetVertex;
    this.partition = jobEdge.getPartition();
    this.executionJobEdgeIndex = generateExecutionJobEdgeIndex();
  }

  private String generateExecutionJobEdgeIndex() {
    return sourceVertex.getJobVertexId() + "—" + targetVertex.getJobVertexId();
  }

  public ExecutionJobVertex getSourceVertex() {
    return sourceVertex;
  }

  public ExecutionJobVertex getTargetVertex() {
    return targetVertex;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcVertex", sourceVertex)
        .add("targetVertex", targetVertex)
        .add("partition", partition)
        .add("executionJobEdgeIndex", executionJobEdgeIndex)
        .toString();
  }
}
