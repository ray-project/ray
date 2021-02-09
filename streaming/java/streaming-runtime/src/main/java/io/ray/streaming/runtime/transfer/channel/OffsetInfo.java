package io.ray.streaming.runtime.transfer.channel;

import com.google.common.base.MoreObjects;
import java.io.Serializable;

/** This data structure contains offset used by streaming queue. */
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

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("streamingMsgId", streamingMsgId).toString();
  }
}
