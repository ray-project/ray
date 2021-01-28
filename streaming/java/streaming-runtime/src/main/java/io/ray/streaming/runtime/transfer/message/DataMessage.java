package io.ray.streaming.runtime.transfer.message;

import java.nio.ByteBuffer;

/** DataMessage represents data between upstream and downstream operators. */
public class DataMessage extends ChannelMessage {

  private final ByteBuffer body;

  public DataMessage(ByteBuffer body, long timestamp, long msgId, String channelId) {
    super(msgId, timestamp, channelId);
    this.body = body;
  }

  public ByteBuffer body() {
    return body;
  }
}
