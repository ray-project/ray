package io.ray.streaming.runtime.worker;

import io.ray.streaming.runtime.config.StreamingWorkerConfig;
import io.ray.streaming.runtime.config.types.TransferChannelType;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.processor.OneInputProcessor;
import io.ray.streaming.runtime.core.processor.ProcessBuilder;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.core.processor.StreamProcessor;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.transfer.TransferHandler;
import io.ray.streaming.runtime.util.EnvUtil;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import io.ray.streaming.runtime.worker.tasks.OneInputStreamTask;
import io.ray.streaming.runtime.worker.tasks.SourceStreamTask;
import io.ray.streaming.runtime.worker.tasks.StreamTask;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The streaming worker implementation class, it is ray actor. JobWorker is created by
 * {@link JobMaster} through ray api, and JobMaster communicates
 * with JobWorker through Ray.call().
 *
 * <p>The JobWorker is responsible for creating tasks and defines the methods of communication
 * between workers.
 */
public class JobWorker implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  // special flag to indicate this actor not ready
  private static final byte[] NOT_READY_FLAG = new byte[4];

  static {
    EnvUtil.loadNativeLibraries();
  }

  private JobWorkerContext workerContext;
  private ExecutionVertex executionVertex;
  private StreamingWorkerConfig workerConfig;

  private StreamTask task;
  private TransferHandler transferHandler;

  public JobWorker() {
    LOG.info("Creating job worker succeeded.");
  }

  /**
   * Initialize JobWorker and data communication pipeline.
   */
  public Boolean init(JobWorkerContext workerContext) {
    LOG.info("Initiating job worker: {}. Worker context is: {}.",
        workerContext.getWorkerName(), workerContext);

    try {
      this.workerContext = workerContext;
      this.executionVertex = workerContext.getExecutionVertex();
      this.workerConfig = new StreamingWorkerConfig(executionVertex.getWorkerConfig());

      //Init transfer
      TransferChannelType channelType = workerConfig.transferConfig.channelType();
      if (TransferChannelType.NATIVE_CHANNEL == channelType) {
        transferHandler = new TransferHandler();
      }

      // create stream task
      task = createStreamTask();
      if (task == null) {
        return false;
      }
    } catch (Exception e) {
      LOG.error("Failed to initiate job worker.", e);
      return false;
    }
    LOG.info("Initiating job worker succeeded: {}.", workerContext.getWorkerName());
    return true;
  }

  /**
   * Start worker's stream tasks.
   *
   * @return result
   */
  public Boolean start() {
    try {
      task.start();
    } catch (Exception e) {
      LOG.error("Start worker [{}] occur error.", executionVertex.getExecutionVertexName(), e);
      return false;
    }
    return true;
  }

  /**
   * Create tasks based on the processor corresponding of the operator.
   */
  private StreamTask createStreamTask() {
    StreamTask task = null;
    StreamProcessor streamProcessor = ProcessBuilder
        .buildProcessor(executionVertex.getStreamOperator());
    LOG.debug("Stream processor created: {}.", streamProcessor);

    try {
      if (streamProcessor instanceof SourceProcessor) {
        task = new SourceStreamTask(getTaskId(), streamProcessor, this);
      } else if (streamProcessor instanceof OneInputProcessor) {
        task = new OneInputStreamTask(getTaskId(), streamProcessor, this);
      } else {
        throw new RuntimeException("Unsupported processor type:" + streamProcessor);
      }
    } catch (Exception e) {
      LOG.info("Failed to create stream task.", e);
      return task;
    }
    LOG.info("Stream task created: {}.", task);
    return task;
  }

  public int getTaskId() {
    return executionVertex.getExecutionVertexId();
  }

  public StreamingWorkerConfig getWorkerConfig() {
    return workerConfig;
  }

  public JobWorkerContext getWorkerContext() {
    return workerContext;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamTask getTask() {
    return task;
  }

  /**
   * Used by upstream streaming queue to send data to this actor
   */
  public void onReaderMessage(byte[] buffer) {
    transferHandler.onReaderMessage(buffer);
  }

  /**
   * Used by upstream streaming queue to send data to this actor
   * and receive result from this actor
   */
  public byte[] onReaderMessageSync(byte[] buffer) {
    if (transferHandler == null) {
      return NOT_READY_FLAG;
    }
    return transferHandler.onReaderMessageSync(buffer);
  }

  /**
   * Used by downstream streaming queue to send data to this actor
   */
  public void onWriterMessage(byte[] buffer) {
    transferHandler.onWriterMessage(buffer);
  }

  /**
   * Used by downstream streaming queue to send data to this actor
   * and receive result from this actor
   */
  public byte[] onWriterMessageSync(byte[] buffer) {
    if (transferHandler == null) {
      return NOT_READY_FLAG;
    }
    return transferHandler.onWriterMessageSync(buffer);
  }
}
