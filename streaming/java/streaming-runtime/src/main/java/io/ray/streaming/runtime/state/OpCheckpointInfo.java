package io.ray.streaming.runtime.state;

import java.io.Serializable;
import java.util.Map;

import io.ray.streaming.runtime.transfer.OffsetInfo;

public class OpCheckpointInfo implements Serializable {
  public Map<String, OffsetInfo> inputPoints;
  public Map<String, OffsetInfo> outputPoints;
  public Object processorCheckpoint;
  public long checkpointId;

  public OpCheckpointInfo() {
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
}
