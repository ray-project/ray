package org.ray.streaming.util;

public class ConfigKey {

  /**
   * Maximum number of batches to run in a streaming job.
   */
  public static final String STREAMING_BATCH_MAX_COUNT = "streaming.batch.max.count";

  /**
   * batch frequency in milliseconds
   */
  public static final String STREAMING_BATCH_FREQUENCY = "streaming.batch.frequency";
  public static final long STREAMING_BATCH_FREQUENCY_DEFAULT = 1000;

  // queue config
  /**
   * Queue type: memory_queue/streaming_queue
   */
  public static final String STREAMING_QUEUE_TYPE = "streaming.queue.type";
  public static final String MEMORY_QUEUE = "memory_queue";
  public static final String STREAMING_QUEUE = "streaming_queue";
  public static final String QUEUE_SIZE = "streaming.queue.size";
  public static final long QUEUE_SIZE_DEFAULT = 100000000L;


}
