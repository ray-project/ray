package io.ray.streaming.jobgraph;

import io.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/** Job edge is connection and partition rules of upstream and downstream execution nodes. */
public class JobEdge implements Serializable {

  private int srcVertexId;
  private int targetVertexId;
  private Partition partition;

  public JobEdge(int srcVertexId, int targetVertexId, Partition partition) {
    this.srcVertexId = srcVertexId;
    this.targetVertexId = targetVertexId;
    this.partition = partition;
  }

  public int getSrcVertexId() {
    return srcVertexId;
  }

  public void setSrcVertexId(int srcVertexId) {
    this.srcVertexId = srcVertexId;
  }

  public int getTargetVertexId() {
    return targetVertexId;
  }

  public void setTargetVertexId(int targetVertexId) {
    this.targetVertexId = targetVertexId;
  }

  public Partition getPartition() {
    return partition;
  }

  public void setPartition(Partition partition) {
    this.partition = partition;
  }

  @Override
  public String toString() {
    return "Edge("
        + "from:"
        + srcVertexId
        + "-"
        + targetVertexId
        + "-"
        + this.partition.getClass()
        + ")";
  }
}
