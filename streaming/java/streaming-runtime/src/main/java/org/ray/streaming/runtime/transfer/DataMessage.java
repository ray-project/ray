package org.ray.streaming.runtime.transfer;

import java.nio.ByteBuffer;

public class DataMessage implements Message {
  private final ByteBuffer body;
  private final long timestamp;
  private final String channelID;

  public DataMessage(ByteBuffer body, long timestamp, String channelID) {
    this.body = body;
    this.timestamp = timestamp;
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


  @Override
  public String toString() {
    return "DataMessage{" +
            "body=" + body +
            ", timestamp=" + timestamp +
            ", channelID='" + channelID + '\'' +
            '}';
  }
}
