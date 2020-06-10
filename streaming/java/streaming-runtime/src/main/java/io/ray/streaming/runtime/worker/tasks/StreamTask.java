package io.ray.streaming.runtime.worker.tasks;

import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.runtime.config.worker.WorkerInternalConfig;
import io.ray.streaming.runtime.core.collector.OutputCollector;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.transfer.ChannelID;
import io.ray.streaming.runtime.transfer.DataReader;
import io.ray.streaming.runtime.transfer.DataWriter;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.StreamingRuntimeContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected int taskId;
  protected Processor processor;
  protected JobWorker jobWorker;
  protected DataReader reader;
  List<Collector> collectors = new ArrayList<>();

  protected volatile boolean running = true;
  protected volatile boolean stopped = false;

  private Thread thread;

  protected StreamTask(int taskId, Processor processor, JobWorker jobWorker) {
    this.taskId = taskId;
    this.processor = processor;
    this.jobWorker = jobWorker;
    prepareTask();

    this.thread = new Thread(Ray.wrapRunnable(this),
      this.getClass().getName() + "-" + System.currentTimeMillis());
    this.thread.setDaemon(true);
  }

  /**
   * Build upstream and downstream data transmission channels according to {@link ExecutionVertex}.
   */
  private void prepareTask() {
    LOG.debug("Preparing stream task.");
    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();

    // set vertex info into config for native using
    jobWorker.getWorkerConfig().workerInternalConfig.setProperty(
        WorkerInternalConfig.WORKER_NAME_INTERNAL, executionVertex.getExecutionVertexName());
    jobWorker.getWorkerConfig().workerInternalConfig.setProperty(
        WorkerInternalConfig.OP_NAME_INTERNAL, executionVertex.getExecutionJobVertexName());

    // producer
    List<ExecutionEdge> outputEdges = executionVertex.getOutputEdges();
    Map<String, BaseActorHandle> outputActors = new HashMap<>();

    for (ExecutionEdge edge : outputEdges) {
      String queueName = ChannelID.genIdStr(
          taskId,
          edge.getTargetExecutionVertex().getExecutionVertexId(),
          executionVertex.getBuildTime());
      outputActors.put(queueName, edge.getTargetExecutionVertex().getWorkerActor());
    }

    if (!outputActors.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      outputActors.forEach((vertexId, actorId) -> {
        channelIDs.add(vertexId);
      });

      DataWriter writer = new DataWriter(channelIDs, outputActors, jobWorker.getWorkerConfig());
      collectors.add(new OutputCollector(writer, channelIDs, outputActors.values(),
          executionVertex.getOutputEdges().get(0).getPartition()));
    }

    // consumer
    List<ExecutionEdge> inputEdges = executionVertex.getInputEdges();
    Map<String, BaseActorHandle> inputActors = new HashMap<>();
    for (ExecutionEdge edge : inputEdges) {
      String queueName = ChannelID.genIdStr(
          edge.getSourceExecutionVertex().getExecutionVertexId(),
          taskId,
          executionVertex.getBuildTime());
      inputActors.put(queueName, edge.getSourceExecutionVertex().getWorkerActor());
    }
    if (!inputActors.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      inputActors.forEach((k, v) -> {
        channelIDs.add(k);
      });
      LOG.info("Register queue consumer, queues {}.", channelIDs);
      reader = new DataReader(channelIDs, inputActors, jobWorker.getWorkerConfig());
    }

    RuntimeContext runtimeContext = new StreamingRuntimeContext(executionVertex,
        jobWorker.getWorkerConfig().configMap, executionVertex.getParallelism());

    processor.open(collectors, runtimeContext);
    LOG.debug("Finished preparing stream task.");
  }

  /**
   * Task initialization related work.
   */
  protected abstract void init() throws Exception;

  /**
   * Stop running tasks.
   */
  protected abstract void cancelTask() throws Exception;

  public void start() {
    LOG.info("Start stream task: {}-{}", this.getClass().getSimpleName(), taskId);
    this.thread.start();
  }

  /**
   * Close running tasks.
   */
  public void close() {
    this.running = false;
    if (thread.isAlive() && !Ray.getRuntimeContext().isSingleProcess()) {
      // `Runtime.halt` is used because System.exist can't ensure the process killing.
      Runtime.getRuntime().halt(0);
      LOG.warn("runtime halt 0");
      System.exit(0);
    }
    LOG.info("Stream task close success.");
  }

}
