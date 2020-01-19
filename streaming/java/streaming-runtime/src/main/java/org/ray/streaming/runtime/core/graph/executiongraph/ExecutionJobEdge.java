package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.jobgraph.JobEdge;

/**
 * Edge to attach execution job vertex.
 */
public class ExecutionJobEdge {

  private final ExecutionJobVertex srcVertex;
  private final ExecutionJobVertex targetVertex;
  private final JobEdge jobEdge;
  private final String executionJobEdgeIndex;

  public ExecutionJobEdge(ExecutionJobVertex srcVertex, ExecutionJobVertex targetVertex,
      JobEdge jobEdge) {
    this.srcVertex = srcVertex;
    this.targetVertex = targetVertex;
    this.jobEdge = jobEdge;
    this.executionJobEdgeIndex = generateExecutionJobEdgeIndex();
  }

  private String generateExecutionJobEdgeIndex() {
    return srcVertex.getJobVertexId() + "—" + targetVertex.getJobVertexId();
  }

  public ExecutionJobVertex getSrcVertex() {
    return srcVertex;
  }

  public ExecutionJobVertex getTargetVertex() {
    return targetVertex;
  }

  public Partition getPartition() {
    return jobEdge.getPartition();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcVertex", srcVertex)
        .add("targetVertex", targetVertex)
        .add("jobEdge", jobEdge)
        .toString();
  }

}
