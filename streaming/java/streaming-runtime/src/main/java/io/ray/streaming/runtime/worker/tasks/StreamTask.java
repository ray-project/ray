package io.ray.streaming.runtime.worker.tasks;

import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.runtime.config.worker.WorkerInternalConfig;
import io.ray.streaming.runtime.core.collector.OutputCollector;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.transfer.ChannelId;
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

    // merge all output edges to create writer
    List<String> outputChannelIds = new ArrayList<>();
    List<BaseActorHandle> targetActors = new ArrayList<>();

    for (ExecutionEdge edge : outputEdges) {
      String channelId = ChannelId.genIdStr(
          taskId,
          edge.getTargetExecutionVertex().getExecutionVertexId(),
          executionVertex.getBuildTime());
      outputChannelIds.add(channelId);
      targetActors.add(edge.getTargetExecutionVertex().getWorkerActor());
    }

    if (!targetActors.isEmpty()) {
      DataWriter writer = new DataWriter(
          outputChannelIds, targetActors, jobWorker.getWorkerConfig()
      );

      // create a collector for each output operator
      Map<String, List<String>> opGroupedChannelId = new HashMap<>();
      Map<String, List<BaseActorHandle>> opGroupedActor = new HashMap<>();
      Map<String, Partition> opPartitionMap = new HashMap<>();
      for (int i = 0; i < outputEdges.size(); ++i) {
        ExecutionEdge edge = outputEdges.get(i);
        String opName = edge.getTargetExecutionJobVertexName();
        if (!opPartitionMap.containsKey(opName)) {
          opGroupedChannelId.put(opName, new ArrayList<>());
          opGroupedActor.put(opName, new ArrayList<>());
        }
        opGroupedChannelId.get(opName).add(outputChannelIds.get(i));
        opGroupedActor.get(opName).add(targetActors.get(i));
        opPartitionMap.put(opName, edge.getPartition());
      }
      opPartitionMap.keySet().forEach(opName -> {
        collectors.add(new OutputCollector(
            writer, opGroupedChannelId.get(opName),
            opGroupedActor.get(opName), opPartitionMap.get(opName)
        ));
      });
    }

    // consumer
    List<ExecutionEdge> inputEdges = executionVertex.getInputEdges();
    List<String> inputChannelIds = new ArrayList<>();
    List<BaseActorHandle> inputActors = new ArrayList<>();
    for (ExecutionEdge edge : inputEdges) {
      String queueName = ChannelId.genIdStr(
          edge.getSourceExecutionVertex().getExecutionVertexId(),
          taskId,
          executionVertex.getBuildTime());
      inputChannelIds.add(queueName);
      inputActors.add(edge.getSourceExecutionVertex().getWorkerActor());
    }
    if (!inputActors.isEmpty()) {
      LOG.info("Register queue consumer, channels {}.", inputChannelIds);
      reader = new DataReader(inputChannelIds, inputActors, jobWorker.getWorkerConfig());
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
