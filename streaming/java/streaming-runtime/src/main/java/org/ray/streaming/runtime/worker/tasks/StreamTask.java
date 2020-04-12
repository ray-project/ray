package org.ray.streaming.runtime.worker.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.BaseActor;
import org.ray.api.Ray;
import org.ray.api.id.ActorId;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.runtime.core.collector.OutputCollector;
import org.ray.streaming.runtime.core.graph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.ExecutionNode;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.transfer.ChannelID;
import org.ray.streaming.runtime.transfer.DataReader;
import org.ray.streaming.runtime.transfer.DataWriter;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.RayRuntimeContext;
import org.ray.streaming.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected int taskId;
  protected Processor processor;
  protected JobWorker worker;
  protected DataReader reader;
  private Map<ExecutionEdge, DataWriter> writers;
  private Thread thread;

  public StreamTask(int taskId, Processor processor, JobWorker worker) {
    this.taskId = taskId;
    this.processor = processor;
    this.worker = worker;
    prepareTask();

    this.thread = new Thread(Ray.wrapRunnable(this), this.getClass().getName()
        + "-" + System.currentTimeMillis());
    this.thread.setDaemon(true);
  }

  private void prepareTask() {
    Map<String, String> queueConf = new HashMap<>();
    worker.getConfig().forEach((k, v) -> queueConf.put(k, String.valueOf(v)));
    String queueSize = (String) worker.getConfig()
        .getOrDefault(Config.CHANNEL_SIZE, Config.CHANNEL_SIZE_DEFAULT);
    queueConf.put(Config.CHANNEL_SIZE, queueSize);
    queueConf.put(Config.TASK_JOB_ID, Ray.getRuntimeContext().getCurrentJobId().toString());
    String channelType = (String) worker.getConfig()
        .getOrDefault(Config.CHANNEL_TYPE, Config.MEMORY_CHANNEL);
    queueConf.put(Config.CHANNEL_TYPE, channelType);

    ExecutionGraph executionGraph = worker.getExecutionGraph();
    ExecutionNode executionNode = worker.getExecutionNode();

    // writers
    writers = new HashMap<>();
    List<ExecutionEdge> outputEdges = executionNode.getOutputEdges();
    List<Collector> collectors = new ArrayList<>();
    for (ExecutionEdge edge : outputEdges) {
      Map<String, ActorId> outputActorIds = new HashMap<>();
      Map<Integer, BaseActor> taskId2Worker = executionGraph
          .getTaskId2WorkerByNodeId(edge.getTargetNodeId());
      taskId2Worker.forEach((targetTaskId, targetActor) -> {
        String queueName = ChannelID.genIdStr(taskId, targetTaskId, executionGraph.getBuildTime());
        outputActorIds.put(queueName, targetActor.getId());
      });

      if (!outputActorIds.isEmpty()) {
        List<String> channelIDs = new ArrayList<>();
        List<ActorId> toActorIds = new ArrayList<>();
        outputActorIds.forEach((k, v) -> {
          channelIDs.add(k);
          toActorIds.add(v);
        });
        DataWriter writer = new DataWriter(channelIDs, toActorIds, queueConf);
        LOG.info("Create DataWriter succeed.");
        writers.put(edge, writer);
        Partition partition = edge.getPartition();
        collectors.add(new OutputCollector(channelIDs, writer, partition));
      }
    }

    // consumer
    List<ExecutionEdge> inputEdges = executionNode.getInputsEdges();
    Map<String, ActorId> inputActorIds = new HashMap<>();
    for (ExecutionEdge edge : inputEdges) {
      Map<Integer, BaseActor> taskId2Worker = executionGraph
          .getTaskId2WorkerByNodeId(edge.getSrcNodeId());
      taskId2Worker.forEach((srcTaskId, srcActor) -> {
        String queueName = ChannelID.genIdStr(srcTaskId, taskId, executionGraph.getBuildTime());
        inputActorIds.put(queueName, srcActor.getId());
      });
    }
    if (!inputActorIds.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      List<ActorId> fromActorIds = new ArrayList<>();
      inputActorIds.forEach((k, v) -> {
        channelIDs.add(k);
        fromActorIds.add(v);
      });
      LOG.info("Register queue consumer, queues {}.", channelIDs);
      reader = new DataReader(channelIDs, fromActorIds, queueConf);
    }

    RuntimeContext runtimeContext = new RayRuntimeContext(
        worker.getExecutionTask(), worker.getConfig(), executionNode.getParallelism());

    processor.open(collectors, runtimeContext);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        // Make DataReader stop read data when MockQueue destructor gets called to avoid crash
        StreamTask.this.cancelTask();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }));
  }

  protected abstract void init() throws Exception;

  protected abstract void cancelTask() throws Exception;

  public void start() {
    this.thread.start();
    LOG.info("started {}-{}", this.getClass().getSimpleName(), taskId);
  }

}
