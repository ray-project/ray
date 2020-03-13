package org.ray.streaming.runtime.worker;

import org.ray.api.Ray;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.config.types.TransferChannelType;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.processor.OneInputProcessor;
import org.ray.streaming.runtime.core.processor.ProcessBuilder;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.core.processor.StreamProcessor;
import org.ray.streaming.runtime.transfer.TransferHandler;
import org.ray.streaming.runtime.util.EnvUtil;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.ray.streaming.runtime.worker.tasks.OneInputStreamTask;
import org.ray.streaming.runtime.worker.tasks.SourceStreamTask;
import org.ray.streaming.runtime.worker.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The streaming worker implementation class, it is ray actor. JobWorker is created by
 * {@link org.ray.streaming.runtime.master.JobMaster} through ray api, and JobMaster communicates
 * with JobWorker through Ray.call().
 *
 * <p>The JobWorker is responsible for creating tasks and defines the methods of communication
 * between workers.
 */
public class JobWorker {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  static {
    EnvUtil.loadNativeLibraries();
  }

  private JobWorkerContext workerContext;
  private ExecutionVertex executionVertex;
  private StreamingWorkerConfig workerConfig;
  private TransferHandler transferHandler;
  private StreamTask task;

  public JobWorker() {
    LOG.info("Job worker init success.");
  }

  /**
   * Initialize JobWorker and data communication pipeline.
   */
  public Boolean init(JobWorkerContext workerContext) {
    LOG.info("Init worker context {}. workerId: {}.", workerContext, workerContext.getWorkerId());
    this.workerContext = workerContext;
    this.executionVertex = workerContext.getExecutionVertex();
    this.workerConfig = executionVertex.getWorkerConfig();

    //Init transfer
    TransferChannelType channelType = workerConfig.transferConfig.channelType();
    if (TransferChannelType.NATIVE_CHANNEL == channelType) {
      transferHandler = new TransferHandler(
          getNativeCoreWorker(),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onWriterMessage", "([B)V"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onWriterMessageSync", "([B)[B"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onReaderMessage", "([B)V"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onReaderMessageSync", "([B)[B"));
    }

    this.task = createStreamTask();

    return true;
  }

  private long getNativeCoreWorker() {
    long pointer = 0;
    if (Ray.internal() instanceof RayMultiWorkerNativeRuntime) {
      pointer = ((RayMultiWorkerNativeRuntime) Ray.internal())
          .getCurrentRuntime().getNativeCoreWorkerPointer();
    }
    return pointer;
  }

  /**
   * Create tasks based on the processor corresponding of the operator.
   */
  private StreamTask createStreamTask() {
    StreamTask task;
    StreamProcessor streamProcessor = ProcessBuilder
        .buildProcessor(executionVertex.getStreamOperator());
    if (streamProcessor instanceof SourceProcessor) {
      task = new SourceStreamTask(executionVertex.getVertexId(), streamProcessor, this);
    } else if (streamProcessor instanceof OneInputProcessor) {
      task = new OneInputStreamTask(executionVertex.getVertexId(), streamProcessor, this);
    } else {
      throw new RuntimeException("Unsupported processor type:" + streamProcessor);
    }
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
    return transferHandler.onReaderMessageSync(buffer);
  }

  /**
   * Used by downstream streaming queue to send data to this actor
   */
  public void onWriterMessage(byte[] buffer) {
    transferHandler.onWriterMessage(buffer);
  }

  /**
   * Start managed tasks.
   */
  public Boolean start() {
    try {
      task.start();
    } catch (Exception e) {
      LOG.error("Start worker [{}] occur error.", executionVertex.getVertexId(), e);
      return false;
    }
    return true;
  }

  public void shutdown() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("Worker shutdown now.");
    }));
    System.exit(0);
  }

  public Boolean destroy() {
    try {
      if (task != null) {
        task.close();
        task = null;
      }
    } catch (Exception e) {
      LOG.error("Close task has exception.", e);
      return false;
    }
    return true;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamingWorkerConfig getWorkerConfig() {
    return workerConfig;
  }
}
