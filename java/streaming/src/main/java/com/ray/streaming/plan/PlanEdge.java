package com.ray.streaming.plan;

import com.ray.streaming.api.partition.Partition;
import java.io.Serializable;

/**
 * PlanEdge is connection and sharding rules of upstream and downstream execution nodes.
 */
public class PlanEdge implements Serializable {

  private int srcVertexId;
  private int targetVertexId;
  private Partition partition;

  public PlanEdge(int srcVertexId, int targetVertexId, Partition partition) {
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
    return "Edge(" + "from:" + srcVertexId + "-" + targetVertexId + "-" + this.partition.getClass()
        + ")";
  }
}
