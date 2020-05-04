package io.ray.streaming.runtime.core.graph;

import io.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/**
 * An edge in the physical execution graph.
 */
public class ExecutionEdge implements Serializable {
  private int srcNodeId;
  private int targetNodeId;
  private Partition partition;

  public ExecutionEdge(int srcNodeId, int targetNodeId, Partition partition) {
    this.srcNodeId = srcNodeId;
    this.targetNodeId = targetNodeId;
    this.partition = partition;
  }

  public int getSrcNodeId() {
    return srcNodeId;
  }

  public void setSrcNodeId(int srcNodeId) {
    this.srcNodeId = srcNodeId;
  }

  public int getTargetNodeId() {
    return targetNodeId;
  }

  public void setTargetNodeId(int targetNodeId) {
    this.targetNodeId = targetNodeId;
  }

  public Partition getPartition() {
    return partition;
  }

  public void setPartition(Partition partition) {
    this.partition = partition;
  }

  public String getStream() {
    return "stream:" + srcNodeId + "-" + targetNodeId;
  }
}
