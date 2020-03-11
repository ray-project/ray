package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;
import org.ray.streaming.runtime.config.types.TransferChannelType;

/**
 * Job data transfer config.
 */
public interface TransferConfig extends Config {

  @DefaultValue(value = "NATIVE_CHANNEL")
  @Key(value = org.ray.streaming.util.Config.CHANNEL_TYPE)
  TransferChannelType channelType();

  @DefaultValue(value = "100000000")
  @Key(value = org.ray.streaming.util.Config.CHANNEL_SIZE)
  long channelSize();

  @DefaultValue(value = "false")
  @Key(value = org.ray.streaming.util.Config.IS_RECREATE)
  boolean readerIsRecreate();

  @DefaultValue(value = "-1")
  @Key(value = org.ray.streaming.util.Config.TIMER_INTERVAL_MS)
  long readerTimerIntervalMs();

  @DefaultValue(value = "-1")
  @Key(value = org.ray.streaming.util.Config.STREAMING_RING_BUFFER_CAPACITY)
  int ringBufferCapacity();

  @DefaultValue(value = "-1")
  @Key(value = org.ray.streaming.util.Config.STREAMING_EMPTY_MESSAGE_INTERVAL)
  int emptyMsgInterval();

  @DefaultValue(value = "-1")
  @Key(value = org.ray.streaming.util.Config.FLOW_CONTROL_TYPE)
  int flowControlType();

  @DefaultValue(value = "-1")
  @Key(value = org.ray.streaming.util.Config.WRITER_CONSUMED_STEP)
  int writerConsumedStep();

  @DefaultValue(value = "-1")
  @Key(value = org.ray.streaming.util.Config.READER_CONSUMED_STEP)
  int readerConsumedStep();
}
