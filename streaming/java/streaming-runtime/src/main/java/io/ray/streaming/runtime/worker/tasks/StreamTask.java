package io.ray.streaming.runtime.worker.tasks;

import io.ray.api.BaseActor;
import io.ray.api.Ray;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.runtime.core.collector.OutputCollector;
import io.ray.streaming.runtime.core.graph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.ExecutionNode;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.transfer.ChannelID;
import io.ray.streaming.runtime.transfer.DataReader;
import io.ray.streaming.runtime.transfer.DataWriter;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.RayRuntimeContext;
import io.ray.streaming.util.Config;

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
  protected JobWorker worker;
  protected DataReader reader;
  private Map<ExecutionEdge, DataWriter> writers;
  private Thread thread;

  public StreamTask(int taskId, Processor processor, JobWorker worker) {
    this.taskId = taskId;
    this.processor = processor;
    this.worker = worker;
    prepareTask();

    this.thread = new Thread(Ray.wrapRunnable(this),
        this.getClass().getName() + "-" + System.currentTimeMillis());
    this.thread.setDaemon(true);
  }

  private void prepareTask() {
    Map<String, String> queueConf = new HashMap<>();
    worker.getConfig().forEach((k, v) -> queueConf.put(k, String.valueOf(v)));
    String queueSize = worker.getConfig()
        .getOrDefault(Config.CHANNEL_SIZE, Config.CHANNEL_SIZE_DEFAULT);
    queueConf.put(Config.CHANNEL_SIZE, queueSize);
    String channelType = worker.getConfig()
        .getOrDefault(Config.CHANNEL_TYPE, Config.MEMORY_CHANNEL);
    queueConf.put(Config.CHANNEL_TYPE, channelType);

    ExecutionGraph executionGraph = worker.getExecutionGraph();
    ExecutionNode executionNode = worker.getExecutionNode();

    // writers
    writers = new HashMap<>();
    List<ExecutionEdge> outputEdges = executionNode.getOutputEdges();
    List<Collector> collectors = new ArrayList<>();
    for (ExecutionEdge edge : outputEdges) {
      Map<String, BaseActor> outputActors = new HashMap<>();
      Map<Integer, BaseActor> taskId2Worker = executionGraph
          .getTaskId2WorkerByNodeId(edge.getTargetNodeId());
      taskId2Worker.forEach((targetTaskId, targetActor) -> {
        String queueName = ChannelID.genIdStr(taskId, targetTaskId, executionGraph.getBuildTime());
        outputActors.put(queueName, targetActor);
      });

      if (!outputActors.isEmpty()) {
        List<String> channelIDs = new ArrayList<>();
        outputActors.forEach((k, v) -> {
          channelIDs.add(k);
        });
        DataWriter writer = new DataWriter(channelIDs, outputActors, queueConf);
        LOG.info("Create DataWriter succeed.");
        writers.put(edge, writer);
        Partition partition = edge.getPartition();
        collectors.add(new OutputCollector(channelIDs, writer, partition));
      }
    }

    // consumer
    List<ExecutionEdge> inputEdges = executionNode.getInputsEdges();
    Map<String, BaseActor> inputActors = new HashMap<>();
    for (ExecutionEdge edge : inputEdges) {
      Map<Integer, BaseActor> taskId2Worker = executionGraph
          .getTaskId2WorkerByNodeId(edge.getSrcNodeId());
      taskId2Worker.forEach((srcTaskId, srcActor) -> {
        String queueName = ChannelID.genIdStr(srcTaskId, taskId, executionGraph.getBuildTime());
        inputActors.put(queueName, srcActor);
      });
    }
    if (!inputActors.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      inputActors.forEach((k, v) -> {
        channelIDs.add(k);
      });
      LOG.info("Register queue consumer, queues {}.", channelIDs);
      reader = new DataReader(channelIDs, inputActors, queueConf);
    }

    RuntimeContext runtimeContext = new RayRuntimeContext(worker.getExecutionTask(),
        worker.getConfig(), executionNode.getParallelism());

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
