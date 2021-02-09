package io.ray.streaming.runtime.transfer.message;

import io.ray.streaming.runtime.transfer.channel.OffsetInfo;
import java.nio.ByteBuffer;
import java.util.Map;

public class BarrierMessage extends ChannelMessage {

  private final ByteBuffer data;
  private final long checkpointId;
  private final Map<String, OffsetInfo> inputOffsets;

  public BarrierMessage(
      long msgId,
      long timestamp,
      String channelId,
      ByteBuffer data,
      long checkpointId,
      Map<String, OffsetInfo> inputOffsets) {
    super(msgId, timestamp, channelId);
    this.data = data;
    this.checkpointId = checkpointId;
    this.inputOffsets = inputOffsets;
  }

  public ByteBuffer getData() {
    return data;
  }

  public long getCheckpointId() {
    return checkpointId;
  }

  public Map<String, OffsetInfo> getInputOffsets() {
    return inputOffsets;
  }
}
