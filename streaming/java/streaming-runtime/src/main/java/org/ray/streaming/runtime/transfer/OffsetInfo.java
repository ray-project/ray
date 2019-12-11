package org.ray.streaming.runtime.transfer;

import java.io.Serializable;

public class OffsetInfo implements Serializable {

  private long seqId;
  private long streamingMsgId;

  public OffsetInfo(long seqId, long streamingMsgId) {
    this.seqId = seqId;
    this.streamingMsgId = streamingMsgId;
  }

  public long getSeqId() {
    return seqId;
  }

  public void setSeqId(long seqId) {
    this.seqId = seqId;
  }

  public long getStreamingMsgId() {
    return streamingMsgId;
  }

  public void setStreamingMsgId(long streamingMsgId) {
    this.streamingMsgId = streamingMsgId;
  }
}
