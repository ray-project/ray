package org.ray.streaming.queue.impl;

public class QueueConfigKeys {
  public static final String PLASMA_STORE_PATH = "storePath";
  public static final String TASK_JOB_ID = "TaskJobID";
  public static final String QUEUE_SIZE = "queueSize";
  public static final String TIMER_INTERVAL_MS = "timerIntervalMs";
  public static final String IS_RECREATE = "isRecreate";

  /**
   * fbs keys
   */

  // string config
  public static final String STREAMING_PERSISTENCE_PATH = "StreamingPersistencePath";
  public static final String STREAMING_JOB_NAME = "StreamingJobName";
  public static final String STREAMING_OP_NAME = "StreamingOpName";
  public static final String STREAMING_WORKER_NAME = "StreamingWorkerName";
  public static final String STREAMING_LOG_PATH = "StreamingLogPath";
  public static final String RAYLET_SOCKET_NAME = "RayletSocketName";
  // uint config
  public static final String STREAMING_LOG_LEVEL = "StreamingLogLevel";
  public static final String STREAMING_RING_BUFFER_CAPACITY = "StreamingRingBufferCapacity";
  public static final String STREAMING_EMPTY_MESSAGE_TIME_INTERVAL = "StreamingEmptyMessageTimeInterval";
  public static final String STREAMING_RECONSTRUCT_OBJECTS_TIME_OUT_PER_MB = "StreamingReconstructObjectsTimeOutPerMb";
  public static final String STREAMING_RECONSTRUCT_OBJECTS_RETRY_TIMES = "StreamingReconstructObjectsRetryTimes";
  public static final String STREAMING_FULL_QUEUE_TIME_INTERVAL = "StreamingFullQueueTimeInterval";
  public static final String CHECKPOINT_ID = "checkpoint_id";
  public static final String STREAMING_WRITER_CONSUMED_STEP = "StreamingWriterConsumedStep";
  public static final String STREAMING_READER_CONSUMED_STEP = "StreamingReaderConsumedStep";
  public static final String RAY_PIPE_BATCH_INTERVAL = "RayPipeBatchInterval";
  public static final String STREAMING_READER_CONSUMED_STEP_UPDATER = "StreamingReaderConsumedStepUpdater";
  public static final String STREAMING_FLOW_CONTROL = "StreamingFlowControl";
}
