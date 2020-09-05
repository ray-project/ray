package io.ray.streaming.runtime.transfer.message;

public class ChannelMessage {

  private final long msgId;
  private final long timestamp;
  private final String channelId;

  public ChannelMessage(long msgId, long timestamp, String channelId) {
    this.msgId = msgId;
    this.timestamp = timestamp;
    this.channelId = channelId;
  }

  public long getMsgId() {
    return msgId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getChannelId() {
    return channelId;
  }
}
