package io.ray.streaming.runtime.transfer;

import java.io.Serializable;

public class OffsetInfo implements Serializable {

  private long streamingMsgId;

  public OffsetInfo(long streamingMsgId) {
    this.streamingMsgId = streamingMsgId;
  }

  public long getStreamingMsgId() {
    return streamingMsgId;
  }

  public void setStreamingMsgId(long streamingMsgId) {
    this.streamingMsgId = streamingMsgId;
  }
}
