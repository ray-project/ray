package io.ray.streaming.runtime.worker;

import io.ray.streaming.runtime.core.graph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.ExecutionNode;
import io.ray.streaming.runtime.core.graph.ExecutionNode.NodeType;
import io.ray.streaming.runtime.core.graph.ExecutionTask;
import io.ray.streaming.runtime.core.processor.OneInputProcessor;
import io.ray.streaming.runtime.core.processor.ProcessBuilder;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.core.processor.StreamProcessor;
import io.ray.streaming.runtime.transfer.TransferHandler;
import io.ray.streaming.runtime.util.EnvUtil;
import io.ray.streaming.runtime.worker.context.WorkerContext;
import io.ray.streaming.runtime.worker.tasks.OneInputStreamTask;
import io.ray.streaming.runtime.worker.tasks.SourceStreamTask;
import io.ray.streaming.runtime.worker.tasks.StreamTask;
import io.ray.streaming.util.Config;

import java.io.Serializable;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The stream job worker, it is a ray actor.
 */
public class JobWorker implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobWorker.class);

  static {
    EnvUtil.loadNativeLibraries();
  }

  private int taskId;
  private Map<String, String> config;
  private WorkerContext workerContext;
  private ExecutionNode executionNode;
  private ExecutionTask executionTask;
  private ExecutionGraph executionGraph;
  private StreamProcessor streamProcessor;
  private NodeType nodeType;
  private StreamTask task;
  private TransferHandler transferHandler;

  public Boolean init(WorkerContext workerContext) {
    this.workerContext = workerContext;
    this.taskId = workerContext.getTaskId();
    this.config = workerContext.getConfig();
    this.executionGraph = this.workerContext.getExecutionGraph();
    this.executionTask = executionGraph.getExecutionTaskByTaskId(taskId);
    this.executionNode = executionGraph.getExecutionNodeByTaskId(taskId);

    this.nodeType = executionNode.getNodeType();
    this.streamProcessor = ProcessBuilder
      .buildProcessor(executionNode.getStreamOperator());
    LOGGER.debug("Initializing StreamWorker, taskId: {}, operator: {}.", taskId, streamProcessor);

    String channelType = (String) this.config.getOrDefault(
        Config.CHANNEL_TYPE, Config.DEFAULT_CHANNEL_TYPE);
    if (channelType.equals(Config.NATIVE_CHANNEL)) {
      transferHandler = new TransferHandler();
    }
    task = createStreamTask();
    task.start();
    return true;
  }

  private StreamTask createStreamTask() {
    if (streamProcessor instanceof OneInputProcessor) {
      return new OneInputStreamTask(taskId, streamProcessor, this);
    } else if (streamProcessor instanceof SourceProcessor) {
      return new SourceStreamTask(taskId, streamProcessor, this);
    } else {
      throw new RuntimeException("Unsupported type: " + streamProcessor);
    }
  }

  public int getTaskId() {
    return taskId;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  public NodeType getNodeType() {
    return nodeType;
  }

  public ExecutionNode getExecutionNode() {
    return executionNode;
  }

  public ExecutionTask getExecutionTask() {
    return executionTask;
  }

  public ExecutionGraph getExecutionGraph() {
    return executionGraph;
  }

  public StreamProcessor getStreamProcessor() {
    return streamProcessor;
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
    return transferHandler.onWriterMessageSync(buffer);
  }
}
