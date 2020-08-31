package io.ray.streaming.runtime.state;

import com.google.common.base.MoreObjects;
import io.ray.streaming.runtime.transfer.channel.OffsetInfo;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OpCheckpointInfo implements Serializable {

  public Map<String, OffsetInfo> inputPoints;
  public Map<String, OffsetInfo> outputPoints;
  public Object processorCheckpoint;
  public long checkpointId;

  public OpCheckpointInfo() {
    inputPoints = new HashMap<>();
    outputPoints = new HashMap<>();
    checkpointId = -1;
  }

  public OpCheckpointInfo(
      Map<String, OffsetInfo> inputPoints,
      Map<String, OffsetInfo> outputPoints,
      Object processorCheckpoint,
      long checkpointId) {
    this.inputPoints = inputPoints;
    this.outputPoints = outputPoints;
    this.checkpointId = checkpointId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("inputPoints", inputPoints)
        .add("outputPoints", outputPoints)
        .add("processorCheckpoint", processorCheckpoint)
        .add("checkpointId", checkpointId)
        .toString();
  }
}
