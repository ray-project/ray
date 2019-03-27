package com.ray.streaming.core.runtime;

import com.ray.streaming.api.collector.Collector;
import com.ray.streaming.core.command.BatchInfo;
import com.ray.streaming.core.graph.ExecutionEdge;
import com.ray.streaming.core.graph.ExecutionGraph;
import com.ray.streaming.core.graph.ExecutionNode;
import com.ray.streaming.core.graph.ExecutionNode.NodeType;
import com.ray.streaming.core.graph.ExecutionTask;
import com.ray.streaming.core.processor.MasterProcessor;
import com.ray.streaming.core.processor.StreamProcessor;
import com.ray.streaming.core.runtime.collector.RayCallCollector;
import com.ray.streaming.core.runtime.context.RayRuntimeContext;
import com.ray.streaming.core.runtime.context.RuntimeContext;
import com.ray.streaming.core.runtime.context.WorkerContext;
import com.ray.streaming.message.Message;
import com.ray.streaming.message.Record;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.annotation.RayRemote;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Stream Worker, it is a ray actor.
 */
@RayRemote
public class StreamWorker implements Serializable {


  private static final Logger LOGGER = LoggerFactory.getLogger(StreamWorker.class);

  private int taskId;
  private WorkerContext workerContext;
  private StreamProcessor streamProcessor;
  private NodeType nodeType;

  public StreamWorker() {
  }

  public Boolean init(WorkerContext workerContext) {
    this.workerContext = workerContext;
    this.taskId = workerContext.getTaskId();
    ExecutionGraph executionGraph = this.workerContext.getExecutionGraph();
    ExecutionTask executionTask = executionGraph.getExecutionTaskByTaskId(taskId);
    ExecutionNode executionNode = executionGraph.getExecutionNodeByTaskId(taskId);

    this.nodeType = executionNode.getNodeType();
    this.streamProcessor = executionNode.getStreamProcessor();
    LOGGER.info("StreamWorker init, taskId:{} Operator:{}", taskId, streamProcessor);

    List<ExecutionEdge> executionEdges = executionNode.getExecutionEdgeList();

    List<Collector> collectors = new ArrayList<>();
    for (ExecutionEdge executionEdge : executionEdges) {
      collectors.add(new RayCallCollector(taskId, executionEdge, executionGraph));
    }

    RuntimeContext runtimeContext = new RayRuntimeContext(executionTask,
        executionNode.getParallelism());
    if (this.nodeType == NodeType.MASTER) {
      ((MasterProcessor) streamProcessor).open(collectors, runtimeContext, executionGraph);
    } else {
      this.streamProcessor.open(collectors, runtimeContext);
    }
    return true;
  }

  public Boolean process(Message message) {
    LOGGER.info("StreamWorker process, taskId:{} message:{}", taskId, message);
    if (nodeType == NodeType.SOURCE) {
      Record record = message.getRecord(0);
      BatchInfo batchInfo = (BatchInfo) record.getValue();
      this.streamProcessor.process(batchInfo.getBatchId());
    } else {
      List<Record> records = message.getRecordList();
      for (Record record : records) {
        record.setBatchId(message.getBatchId());
        record.setStream(message.getStream());
        this.streamProcessor.process(record);
      }
    }
    return true;
  }

}
