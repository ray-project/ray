package io.ray.streaming.runtime.core.graph;

import io.ray.api.BaseActor;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Physical execution graph.
 *
 * <p>Notice: Temporary implementation for now to keep functional. This will be changed to
 * {@link ExecutionGraph} later when new stream task implementation is ready.
 */
public class ExecutionGraph implements Serializable {
  private long buildTime;
  private List<ExecutionNode> executionNodeList;
  private List<BaseActor> sourceWorkers = new ArrayList<>();
  private List<BaseActor> sinkWorkers = new ArrayList<>();

  public ExecutionGraph(List<ExecutionNode> executionNodes) {
    this.executionNodeList = executionNodes;
    for (ExecutionNode executionNode : executionNodeList) {
      if (executionNode.getNodeType() == ExecutionNode.NodeType.SOURCE) {
        List<BaseActor> actors = executionNode.getExecutionTasks().stream()
            .map(ExecutionTask::getWorker).collect(Collectors.toList());
        sourceWorkers.addAll(actors);
      }
      if (executionNode.getNodeType() == ExecutionNode.NodeType.SINK) {
        List<BaseActor> actors = executionNode.getExecutionTasks().stream()
            .map(ExecutionTask::getWorker).collect(Collectors.toList());
        sinkWorkers.addAll(actors);
      }
    }
    buildTime = System.currentTimeMillis();
  }

  public List<BaseActor> getSourceWorkers() {
    return sourceWorkers;
  }

  public List<BaseActor> getSinkWorkers() {
    return sinkWorkers;
  }

  public List<ExecutionNode> getExecutionNodeList() {
    return executionNodeList;
  }

  public ExecutionTask getExecutionTaskByTaskId(int taskId) {
    for (ExecutionNode executionNode : executionNodeList) {
      for (ExecutionTask executionTask : executionNode.getExecutionTasks()) {
        if (executionTask.getTaskId() == taskId) {
          return executionTask;
        }
      }
    }
    throw new RuntimeException("Task " + taskId + " does not exist!");
  }

  public ExecutionNode getExecutionNodeByNodeId(int nodeId) {
    for (ExecutionNode executionNode : executionNodeList) {
      if (executionNode.getNodeId() == nodeId) {
        return executionNode;
      }
    }
    throw new RuntimeException("Node " + nodeId + " does not exist!");
  }

  public ExecutionNode getExecutionNodeByTaskId(int taskId) {
    for (ExecutionNode executionNode : executionNodeList) {
      for (ExecutionTask executionTask : executionNode.getExecutionTasks()) {
        if (executionTask.getTaskId() == taskId) {
          return executionNode;
        }
      }
    }
    throw new RuntimeException("Task " + taskId + " does not exist!");
  }

  public Map<Integer, BaseActor> getTaskId2WorkerByNodeId(int nodeId) {
    for (ExecutionNode executionNode : executionNodeList) {
      if (executionNode.getNodeId() == nodeId) {
        Map<Integer, BaseActor> taskId2Worker = new HashMap<>();
        for (ExecutionTask executionTask : executionNode.getExecutionTasks()) {
          taskId2Worker.put(executionTask.getTaskId(), executionTask.getWorker());
        }
        return taskId2Worker;
      }
    }
    throw new RuntimeException("Node " + nodeId + " does not exist!");
  }

  public long getBuildTime() {
    return buildTime;
  }
}
