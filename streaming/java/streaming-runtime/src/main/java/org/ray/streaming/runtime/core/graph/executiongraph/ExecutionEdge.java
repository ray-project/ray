package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.ray.streaming.api.partition.Partition;

/**
 * Edge to attach execution vertex.
 */
public class ExecutionEdge implements Serializable {

  private final ExecutionVertex srcVertex;
  private final ExecutionVertex targetVertex;
  private final ExecutionJobEdge executionJobEdge;
  private final String executionEdgeIndex;

  public ExecutionEdge(ExecutionVertex srcVertex, ExecutionVertex targetVertex,
      ExecutionJobEdge executionJobEdge) {
    this.srcVertex = srcVertex;
    this.targetVertex = targetVertex;
    this.executionJobEdge = executionJobEdge;
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return srcVertex.getVertexId() + "—" + targetVertex.getVertexId();
  }

  public ExecutionVertex getSrcVertex() {
    return srcVertex;
  }

  public ExecutionVertex getTargetVertex() {
    return targetVertex;
  }

  public int getProducerId() {
    return srcVertex.getVertexId();
  }

  public int getConsumerId() {
    return targetVertex.getVertexId();
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  public ExecutionJobEdge getExecutionJobEdge() {
    return executionJobEdge;
  }

  public Partition getPartition() {
    return executionJobEdge.getPartition();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("srcVertex", srcVertex)
        .add("targetVertex", targetVertex)
        .add("executionJobEdge", executionJobEdge)
        .add("executionEdgeIndex", executionEdgeIndex)
        .toString();
  }

}