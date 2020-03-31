package io.ray.streaming.runtime.transfer;

import java.nio.ByteBuffer;

/**
 * DataMessage represents data between upstream and downstream operator
 */
public class DataMessage implements Message {
  private final ByteBuffer body;
  private final long msgId;
  private final long timestamp;
  private final String channelId;

  public DataMessage(ByteBuffer body, long timestamp, long msgId, String channelId) {
    this.body = body;
    this.timestamp = timestamp;
    this.msgId = msgId;
    this.channelId = channelId;
  }

  @Override
  public ByteBuffer body() {
    return body;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  /**
   * @return message id
   */
  public long msgId() {
    return msgId;
  }

  /**
   * @return string id of channel where data is coming from
   */
  public String channelId() {
    return channelId;
  }

  @Override
  public String toString() {
    return "DataMessage{" +
        "body=" + body +
        ", msgId=" + msgId +
        ", timestamp=" + timestamp +
        ", channelId='" + channelId + '\'' +
        '}';
  }
}
