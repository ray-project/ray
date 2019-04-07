package org.ray.streaming.core.runtime.collector;

import java.util.Arrays;
import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.core.graph.ExecutionEdge;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.message.Message;
import org.ray.streaming.message.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The collector that emits data via Ray remote calls.
 */
public class RayCallCollector implements Collector<Record> {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayCallCollector.class);

  private int taskId;
  private String stream;
  private Map<Integer, RayActor<StreamWorker>> taskId2Worker;
  private int[] targetTaskIds;
  private Partition partition;

  public RayCallCollector(int taskId, ExecutionEdge executionEdge, ExecutionGraph executionGraph) {
    this.taskId = taskId;
    this.stream = executionEdge.getStream();
    int targetNodeId = executionEdge.getTargetNodeId();
    taskId2Worker = executionGraph
        .getTaskId2WorkerByNodeId(targetNodeId);
    targetTaskIds = Arrays.stream(taskId2Worker.keySet()
        .toArray(new Integer[taskId2Worker.size()]))
        .mapToInt(Integer::valueOf).toArray();

    this.partition = executionEdge.getPartition();
    LOGGER.debug("RayCallCollector constructed, taskId:{}, add stream:{}, partition:{}.",
        taskId, stream, this.partition);
  }

  @Override
  public void collect(Record record) {
    int[] taskIds = this.partition.partition(record, targetTaskIds);
    LOGGER.debug("Sending data from task {} to remote tasks {}, collector stream:{}, record:{}",
        taskId, taskIds, stream, record);
    Message message = new Message(taskId, record.getBatchId(), stream, record);
    for (int targetTaskId : taskIds) {
      RayActor<StreamWorker> streamWorker = this.taskId2Worker.get(targetTaskId);
      // Use ray call to send message to downstream actor.
      Ray.call(StreamWorker::process, streamWorker, message);
    }
  }

}
