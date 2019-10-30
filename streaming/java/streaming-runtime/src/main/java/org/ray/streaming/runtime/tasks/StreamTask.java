package org.ray.streaming.runtime.tasks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.core.graph.ExecutionEdge;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.graph.ExecutionNode;
import org.ray.streaming.core.processor.Processor;
import org.ray.streaming.queue.QueueConsumer;
import org.ray.streaming.queue.QueueLink;
import org.ray.streaming.queue.QueueProducer;
import org.ray.streaming.queue.QueueUtils;
import org.ray.streaming.queue.impl.StreamingQueueLinkImpl;
import org.ray.streaming.queue.memory.MemQueueLinkImpl;
import org.ray.streaming.runtime.JobWorker;
import org.ray.streaming.runtime.collector.OutputCollector;
import org.ray.streaming.runtime.context.RayRuntimeContext;
import org.ray.streaming.runtime.context.RuntimeContext;
import org.ray.streaming.util.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StreamTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected int taskId;
  protected Processor processor;
  protected JobWorker worker;
  private QueueLink queueLink;
  protected QueueConsumer consumer;
  private Map<ExecutionEdge, QueueProducer> producers;
  private Thread t;

  public StreamTask(int taskId, Processor processor, JobWorker worker) {
    this.taskId = taskId;
    this.processor = processor;
    this.worker = worker;
    prepareTask();

    this.t = new Thread(Ray.wrapRunnable(this), this.getClass().getName()
        + "-" + System.currentTimeMillis());
  }

  private void prepareTask() {
    String queueType = (String) worker.getConfig()
        .getOrDefault(ConfigKey.STREAMING_QUEUE_TYPE, ConfigKey.MEMORY_QUEUE);
    if (ConfigKey.STREAMING_QUEUE.equals(queueType)) {
      queueLink = new StreamingQueueLinkImpl();
    } else {
      queueLink = new MemQueueLinkImpl();
    }
    Map<String, String> queueConf = new HashMap<>();
    String queueSize = (String) worker.getConfig()
        .getOrDefault(ConfigKey.QUEUE_SIZE, ConfigKey.QUEUE_SIZE_DEFAULT + "");
    queueConf.put(ConfigKey.QUEUE_SIZE, queueSize);
    queueLink.setConfiguration(queueConf);
    queueLink.setRayRuntime(Ray.internal());

    ExecutionGraph executionGraph = worker.getExecutionGraph();
    ExecutionNode executionNode = worker.getExecutionNode();

    // queue producers
    producers = new HashMap<>();
    List<ExecutionEdge> outputEdges = executionNode.getOutputEdges();
    List<Collector> collectors = new ArrayList<>();
    for (ExecutionEdge edge : outputEdges) {
      Map<String, Long> outputHandles = new HashMap<>();
      Map<Integer, RayActor<JobWorker>> taskId2Worker = executionGraph
          .getTaskId2WorkerByNodeId(edge.getTargetNodeId());
      taskId2Worker.forEach((targetTaskId, targetActor) -> {
        String queueName = QueueUtils.genQueueName(taskId, targetTaskId, executionGraph.getBuildTime());
        outputHandles.put(queueName, getNativeActorHandle(targetActor));
      });

      Set<String> queueIds = outputHandles.keySet();
      if (!outputHandles.isEmpty()) {
        LOG.info("Register queue producer, queues {}.", queueIds);
        QueueProducer producer = queueLink.registerQueueProducer(queueIds, outputHandles);
        producers.put(edge, producer);
        collectors.add(new OutputCollector(queueIds, producer, edge.getPartition()));
      }
    }

    // queue consumer
    List<ExecutionEdge> inputEdges = executionNode.getInputsEdges();
    Map<String, Long> inputHandles = new HashMap<>();
    for (ExecutionEdge edge : inputEdges) {
      Map<Integer, RayActor<JobWorker>> taskId2Worker = executionGraph
          .getTaskId2WorkerByNodeId(edge.getSrcNodeId());
      taskId2Worker.forEach((srcTaskId, srcActor) -> {
        String queueName = QueueUtils.genQueueName(srcTaskId, taskId, executionGraph.getBuildTime());
        inputHandles.put(queueName, getNativeActorHandle(srcActor));
      });
    }
    if (!inputHandles.isEmpty()) {
      Set<String> queueIds = inputHandles.keySet();
      LOG.info("Register queue consumer, queues {}.", queueIds);
      consumer = queueLink.registerQueueConsumer(queueIds, inputHandles);
    }

    RuntimeContext runtimeContext = new RayRuntimeContext(
        worker.getExecutionTask(), worker.getConfig(), executionNode.getParallelism());

    processor.open(collectors, runtimeContext);

  }

  // return 0 in SINGLE_PROCESS mode
  private static long getNativeActorHandle(RayActor actor) {
    if (actor instanceof NativeRayActor) {
      return ((NativeRayActor) actor).getNativeActorHandle();
    } else {
      return 0;
    }
  }

  protected abstract void init() throws Exception;

  protected abstract void cancelTask() throws Exception;

  public void start() {
    this.t.start();
    LOG.info("started {}-{}", this.getClass().getSimpleName(), taskId);
  }

}
