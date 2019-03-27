package com.ray.streaming.core.processor;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.core.command.BatchInfo;
import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.graph.ExecutionNode;
import com.ray.streaming.core.graph.ExecutionNode.NodeType;
import com.ray.streaming.core.graph.ExecutionTask;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.message.Record;
import com.ray.streaming.operator.impl.MasterOperator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JobMaster Processor is responsible for overall control logic.
 *
 */
public class MasterProcessor extends StreamProcessor<BatchInfo, MasterOperator> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MasterProcessor.class);

  private Thread batchControllerThread;

  public MasterProcessor(MasterOperator masterOperator) {
    super(masterOperator);
  }

  public void open(List<Collector> collectors, RuntimeContext runtimeContext,
      ExecutionGraph executionGraph) {
    super.open(collectors, runtimeContext);
    startBatchController(executionGraph);
  }

  private void startBatchController(ExecutionGraph executionGraph) {
    BatchController batchController = new BatchController(collectors);
    List<Integer> sinkTasks = new ArrayList<>();
    for (ExecutionNode executionNode : executionGraph.getExecutionNodeList()) {
      if (executionNode.getNodeType() == NodeType.SINK) {
        List<Integer> nodeTasks = executionNode.getExecutionTaskList().stream()
            .map(ExecutionTask::getTaskId).collect(Collectors.toList());
        sinkTasks.addAll(nodeTasks);
      }
    }
    batchController.initController(sinkTasks);

    batchControllerThread = new Thread(batchController, "controller-thread");
    batchControllerThread.start();
  }

  @Override
  public void process(BatchInfo executionGraph) {

  }

  @Override
  public void close() {

  }

  static class BatchController implements Runnable, Serializable {

    private AtomicInteger batchId;
    private List<Collector> collectors;
    private Map<Integer, Integer> sinkBatchMap;
    private Integer frequency;

    public BatchController(List<Collector> collectors) {
      this.batchId = new AtomicInteger(1);
      this.collectors = collectors;
      this.frequency = 1000;
    }

    public void initController(List<Integer> sinkTaskIds) {
      sinkBatchMap = new ConcurrentHashMap<>();
      for (Integer sinkTaskId : sinkTaskIds) {
        sinkBatchMap.put(sinkTaskId, 0);
      }
    }

    @Override
    public void run() {
      while (true) {
        try {
          Record record = new Record<>(new BatchInfo(batchId.getAndIncrement()));
          for (Collector collector : collectors) {
            collector.collect(record);
          }
          Thread.sleep(frequency);
        } catch (Exception e) {
          LOGGER.error(e.getMessage(), e);
        }
      }
    }

    public int currentSinkBatch() {
      return Collections.min(sinkBatchMap.values());
    }

    public void batchFinish(int taskId, int batchId) {
      this.sinkBatchMap.put(taskId, batchId);
    }

  }
}
