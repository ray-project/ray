package org.ray.streaming.runtime.worker.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.id.ActorId;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.core.collector.OutputCollector;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.transfer.ChannelID;
import org.ray.streaming.runtime.transfer.DataReader;
import org.ray.streaming.runtime.transfer.DataWriter;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task execution abstract class.
 */
public abstract class StreamTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected int taskId;
  protected Processor processor;
  protected JobWorker jobWorker;
  protected DataReader reader;

  List<Collector> collectors = new ArrayList<>();

  protected volatile boolean running = true;
  protected volatile boolean stopped = false;

  //Execution thread
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

  private void prepareTask() {
    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();

    // consumer
    List<ExecutionEdge> outputEdges = executionVertex.getOutputEdges();
    Map<String, ActorId> outputActor = new HashMap<>();
    for (ExecutionEdge edge : outputEdges) {
      String queueName = ChannelID.genIdStr(
          taskId, edge.getTargetVertex().getVertexId(), executionVertex.getBuildTime());
      outputActor.put(queueName, edge.getTargetVertex().getWorkerActorId());
    }
    if (!outputActor.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      List<ActorId> targetActorIds = new ArrayList<>();
      outputActor.forEach((vertexId, actorId) -> {
        channelIDs.add(vertexId);
        targetActorIds.add(actorId);
      });
      DataWriter writer = new DataWriter(channelIDs, targetActorIds, jobWorker.getWorkerConfig());
      collectors.add(new OutputCollector(channelIDs, writer,
          executionVertex.getOutputEdges().get(0).getPartition()));
    }

    // producer
    List<ExecutionEdge> inputEdges = executionVertex.getInputEdges();
    Map<String, ActorId> inputActorIds = new HashMap<>();
    for (ExecutionEdge edge : inputEdges) {
      String queueName = ChannelID.genIdStr(
          edge.getSourceVertex().getVertexId(), taskId, executionVertex.getBuildTime());
      inputActorIds.put(queueName, edge.getSourceVertex().getWorkerActorId());
    }
    if (!inputActorIds.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      List<ActorId> fromActorIds = new ArrayList<>();
      inputActorIds.forEach((k, v) -> {
        channelIDs.add(k);
        fromActorIds.add(v);
      });
      LOG.info("Register queue consumer, queues {}.", channelIDs);
      reader = new DataReader(channelIDs, fromActorIds, jobWorker.getWorkerConfig());
    }

    RuntimeContext runtimeContext = new StreamingRuntimeContext(executionVertex,
        jobWorker.getWorkerConfig().configMap, executionVertex.getParallelism());
    processor.open(collectors, runtimeContext);
  }

  protected abstract void init() throws Exception;

  protected abstract void cancelTask() throws Exception;

  public void start() {
    this.thread.start();
    LOG.info("Start stream task: {}-{}", this.getClass().getSimpleName(), taskId);
  }

  public void close() {
    this.running = false;
    if (thread.isAlive() && !Ray.getRuntimeContext().isSingleProcess()) {
      Runtime.getRuntime().halt(0);
      System.exit(0);
      LOG.warn("runtime halt 0");
    }
    LOG.info("Stream task close success.");
  }
}
