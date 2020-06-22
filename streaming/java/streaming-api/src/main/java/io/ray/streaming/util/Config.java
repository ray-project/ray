package io.ray.streaming.util;

public class Config {

  public static final String STREAMING_JOB_NAME = "streaming.job.name";
  public static final String STREAMING_OP_NAME = "streaming.op_name";
  public static final String STREAMING_WORKER_NAME = "streaming.worker_name";

  // channel
  public static final String CHANNEL_TYPE = "channel_type";
  public static final String MEMORY_CHANNEL = "memory_channel";
  public static final String NATIVE_CHANNEL = "native_channel";
  public static final String CHANNEL_SIZE = "channel_size";
  public static final String CHANNEL_SIZE_DEFAULT = String.valueOf((long)Math.pow(10, 8));
  public static final String IS_RECREATE = "streaming.is_recreate";
  // return from DataReader.getBundle if only empty message read in this interval.
  public static final String TIMER_INTERVAL_MS = "timer_interval_ms";
  public static final String READ_TIMEOUT_MS = "read_timeout_ms";
  public static final String DEFAULT_READ_TIMEOUT_MS = "10";


  public static final String STREAMING_RING_BUFFER_CAPACITY = "streaming.ring_buffer_capacity";
  // write an empty message if there is no data to be written in this
  // interval.
  public static final String STREAMING_EMPTY_MESSAGE_INTERVAL = "streaming.empty_message_interval";

  // operator type
  public static final String OPERATOR_TYPE = "operator_type";

  // flow control
  public static final String FLOW_CONTROL_TYPE = "streaming.flow_control_type";
  public static final String WRITER_CONSUMED_STEP = "streaming.writer.consumed_step";
  public static final String READER_CONSUMED_STEP = "streaming.reader.consumed_step";


}
