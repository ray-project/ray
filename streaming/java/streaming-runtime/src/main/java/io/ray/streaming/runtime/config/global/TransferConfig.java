package io.ray.streaming.runtime.config.global;

import io.ray.streaming.runtime.config.Config;
import io.ray.streaming.runtime.config.types.TransferChannelType;

/**
 * Job data transfer config.
 */
public interface TransferConfig extends Config {

  /**
   * Data transfer channel type, support memory queue and native queue.
   */
  @DefaultValue(value = "NATIVE_CHANNEL")
  @Key(value = io.ray.streaming.util.Config.CHANNEL_TYPE)
  TransferChannelType channelType();

  /**
   * Queue size.
   */
  @DefaultValue(value = "100000000")
  @Key(value = io.ray.streaming.util.Config.CHANNEL_SIZE)
  long channelSize();

  /**
   * DataRead read timeout.
   */
  @DefaultValue(value = "false")
  @Key(value = io.ray.streaming.util.Config.IS_RECREATE)
  boolean readerIsRecreate();

  /**
   * Return from DataReader.getBundle if only empty message read in this interval.
   */
  @DefaultValue(value = "-1")
  @Key(value = io.ray.streaming.util.Config.TIMER_INTERVAL_MS)
  long readerTimerIntervalMs();

  /**
   * Ring capacity.
   */
  @DefaultValue(value = "-1")
  @Key(value = io.ray.streaming.util.Config.STREAMING_RING_BUFFER_CAPACITY)
  int ringBufferCapacity();

  /**
   * Write an empty message if there is no data to be written in this interval.
   */
  @DefaultValue(value = "-1")
  @Key(value = io.ray.streaming.util.Config.STREAMING_EMPTY_MESSAGE_INTERVAL)
  int emptyMsgInterval();

  // Flow control

  @DefaultValue(value = "-1")
  @Key(value = io.ray.streaming.util.Config.FLOW_CONTROL_TYPE)
  int flowControlType();

  @DefaultValue(value = "-1")
  @Key(value = io.ray.streaming.util.Config.WRITER_CONSUMED_STEP)
  int writerConsumedStep();

  @DefaultValue(value = "-1")
  @Key(value = io.ray.streaming.util.Config.READER_CONSUMED_STEP)
  int readerConsumedStep();
}
