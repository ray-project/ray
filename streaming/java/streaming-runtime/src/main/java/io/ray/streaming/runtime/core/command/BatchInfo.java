package io.ray.streaming.runtime.core.command;

import java.io.Serializable;

public class BatchInfo implements Serializable {

  private long batchId;

  public BatchInfo(long batchId) {
    this.batchId = batchId;
  }

  public long getBatchId() {
    return batchId;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }
}
