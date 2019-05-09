package org.ray.streaming.core.processor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.core.command.BatchInfo;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.graph.ExecutionNode;
import org.ray.streaming.core.graph.ExecutionNode.NodeType;
import org.ray.streaming.core.graph.ExecutionTask;
import org.ray.streaming.core.runtime.context.RuntimeContext;
import org.ray.streaming.message.Record;
import org.ray.streaming.operator.impl.MasterOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MasterProcessor is responsible for overall control logic.
 */
public class MasterProcessor extends StreamProcessor<BatchInfo, MasterOperator> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MasterProcessor.class);

  private Thread batchControllerThread;
  private long maxBatch;

  public MasterProcessor(MasterOperator masterOperator) {
    super(masterOperator);
  }

  public void open(List<Collector> collectors, RuntimeContext runtimeContext,
      ExecutionGraph executionGraph) {
    super.open(collectors, runtimeContext);
    this.maxBatch = runtimeContext.getMaxBatch();
    startBatchController(executionGraph);

  }

  private void startBatchController(ExecutionGraph executionGraph) {
    BatchController batchController = new BatchController(maxBatch, collectors);
    List<Integer> sinkTasks = new ArrayList<>();
    for (ExecutionNode executionNode : executionGraph.getExecutionNodeList()) {
      if (executionNode.getNodeType() == NodeType.SINK) {
        List<Integer> nodeTasks = executionNode.getExecutionTaskList().stream()
            .map(ExecutionTask::getTaskId).collect(Collectors.toList());
        sinkTasks.addAll(nodeTasks);
      }
    }

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
    private long maxBatch;

    public BatchController(long maxBatch, List<Collector> collectors) {
      this.batchId = new AtomicInteger(0);
      this.maxBatch = maxBatch;
      this.collectors = collectors;
      // TODO(zhenxuanpan): Use config to set.
      this.frequency = 1000;
    }

    @Override
    public void run() {
      while (batchId.get() < maxBatch) {
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

  }
}
