package org.ray.streaming.runtime.worker;

import java.io.Serializable;
import java.util.Map;

import org.ray.api.annotation.RayRemote;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.graph.ExecutionNode.NodeType;
import org.ray.streaming.runtime.core.graph.ExecutionTask;
import org.ray.streaming.runtime.core.processor.OneInputProcessor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.core.processor.StreamProcessor;
import org.ray.streaming.runtime.worker.context.WorkerContext;
import org.ray.streaming.runtime.worker.tasks.OneInputStreamTask;
import org.ray.streaming.runtime.worker.tasks.SourceStreamTask;
import org.ray.streaming.runtime.worker.tasks.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The stream job worker, it is a ray actor.
 */
@RayRemote
public class JobWorker implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobWorker.class);

  private int taskId;
  private Map<String, Object> config;
  private WorkerContext workerContext;
  private ExecutionNode executionNode;
  private ExecutionTask executionTask;
  private ExecutionGraph executionGraph;
  private StreamProcessor streamProcessor;
  private NodeType nodeType;
  private StreamTask task;

  public JobWorker() {
  }

  public Boolean init(WorkerContext workerContext) {
    this.workerContext = workerContext;
    this.taskId = workerContext.getTaskId();
    this.config = workerContext.getConfig();
    this.executionGraph = this.workerContext.getExecutionGraph();
    this.executionTask = executionGraph.getExecutionTaskByTaskId(taskId);
    this.executionNode = executionGraph.getExecutionNodeByTaskId(taskId);

    this.nodeType = executionNode.getNodeType();
    this.streamProcessor = executionNode.getStreamProcessor();
    LOGGER.debug("Initializing StreamWorker, taskId: {}, operator: {}.", taskId, streamProcessor);

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

  public Map<String, Object> getConfig() {
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

  // TODO add TransferHandler
}
