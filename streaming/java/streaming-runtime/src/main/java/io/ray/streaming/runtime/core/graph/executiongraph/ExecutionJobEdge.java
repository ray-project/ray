package io.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.jobgraph.JobEdge;
import java.io.Serializable;

/** An edge that connects two execution job vertices. */
public class ExecutionJobEdge implements Serializable {

  /** The source(upstream) execution job vertex. */
  private final ExecutionJobVertex sourceExecutionJobVertex;

  /** The target(downstream) execution job vertex. */
  private final ExecutionJobVertex targetExecutionJobVertex;

  /** The partition of the execution job edge. */
  private final Partition partition;

  /** An unique id for execution job edge. */
  private final String executionJobEdgeIndex;

  public ExecutionJobEdge(
      ExecutionJobVertex sourceExecutionJobVertex,
      ExecutionJobVertex targetExecutionJobVertex,
      JobEdge jobEdge) {
    this.sourceExecutionJobVertex = sourceExecutionJobVertex;
    this.targetExecutionJobVertex = targetExecutionJobVertex;
    this.partition = jobEdge.getPartition();
    this.executionJobEdgeIndex = generateExecutionJobEdgeIndex();
  }

  private String generateExecutionJobEdgeIndex() {
    return sourceExecutionJobVertex.getExecutionJobVertexId()
        + "—"
        + targetExecutionJobVertex.getExecutionJobVertexId();
  }

  public ExecutionJobVertex getSourceExecutionJobVertex() {
    return sourceExecutionJobVertex;
  }

  public ExecutionJobVertex getTargetExecutionJobVertex() {
    return targetExecutionJobVertex;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("source", sourceExecutionJobVertex)
        .add("target", targetExecutionJobVertex)
        .add("partition", partition)
        .add("index", executionJobEdgeIndex)
        .toString();
  }
}
