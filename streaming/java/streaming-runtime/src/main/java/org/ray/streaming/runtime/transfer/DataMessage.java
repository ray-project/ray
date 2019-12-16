package org.ray.streaming.runtime.transfer;

import java.nio.ByteBuffer;

/**
 * DataMessage represents data between upstream and downstream operator
 */
public class DataMessage implements Message {
  private final ByteBuffer body;
  private final long msgId;
  private final long timestamp;
  private final String channelID;

  public DataMessage(ByteBuffer body, long timestamp, long msgId, String channelID) {
    this.body = body;
    this.timestamp = timestamp;
    this.msgId = msgId;
    this.channelID = channelID;
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
  public String channelID() {
    return channelID;
  }

  @Override
  public String toString() {
    return "DataMessage{" +
        "body=" + body +
        ", msgId=" + msgId +
        ", timestamp=" + timestamp +
        ", channelID='" + channelID + '\'' +
        '}';
  }
}
