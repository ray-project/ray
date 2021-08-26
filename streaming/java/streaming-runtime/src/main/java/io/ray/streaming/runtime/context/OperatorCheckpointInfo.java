package io.ray.streaming.runtime.context;

import com.google.common.base.MoreObjects;
import io.ray.streaming.runtime.transfer.channel.OffsetInfo;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** This data structure contains state information of a task. */
public class OperatorCheckpointInfo implements Serializable {

  /** key: channel ID, value: offset */
  public Map<String, OffsetInfo> inputPoints;

  public Map<String, OffsetInfo> outputPoints;

  /** a serializable checkpoint returned by processor */
  public Serializable processorCheckpoint;

  public long checkpointId;

  public OperatorCheckpointInfo() {
    inputPoints = new HashMap<>();
    outputPoints = new HashMap<>();
    checkpointId = -1;
  }

  public OperatorCheckpointInfo(
      Map<String, OffsetInfo> inputPoints,
      Map<String, OffsetInfo> outputPoints,
      Serializable processorCheckpoint,
      long checkpointId) {
    this.inputPoints = inputPoints;
    this.outputPoints = outputPoints;
    this.checkpointId = checkpointId;
    this.processorCheckpoint = processorCheckpoint;
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
