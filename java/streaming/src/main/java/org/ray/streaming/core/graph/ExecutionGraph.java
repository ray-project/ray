package org.ray.streaming.core.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.streaming.core.runtime.StreamWorker;

/**
 * Physical execution graph.
 */
public class ExecutionGraph implements Serializable {

  private List<ExecutionNode> executionNodeList;

  public ExecutionGraph(List<ExecutionNode> executionNodes) {
    this.executionNodeList = executionNodes;
  }

  public void addExectionNode(ExecutionNode executionNode) {
    this.executionNodeList.add(executionNode);
  }

  public List<ExecutionNode> getExecutionNodeList() {
    return executionNodeList;
  }

  public ExecutionTask getExecutionTaskByTaskId(int taskId) {
    for (ExecutionNode executionNode : executionNodeList) {
      for (ExecutionTask executionTask : executionNode.getExecutionTaskList()) {
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
      for (ExecutionTask executionTask : executionNode.getExecutionTaskList()) {
        if (executionTask.getTaskId() == taskId) {
          return executionNode;
        }
      }
    }
    throw new RuntimeException("Task " + taskId + " does not exist!");
  }

  public Map<Integer, RayActor<StreamWorker>> getTaskId2WorkerByNodeId(int nodeId) {
    for (ExecutionNode executionNode : executionNodeList) {
      if (executionNode.getNodeId() == nodeId) {
        Map<Integer, RayActor<StreamWorker>> taskId2Worker = new HashMap<>();
        for (ExecutionTask executionTask : executionNode.getExecutionTaskList()) {
          taskId2Worker.put(executionTask.getTaskId(), executionTask.getWorker());
        }
        return taskId2Worker;
      }
    }
    throw new RuntimeException("Node " + nodeId + " does not exist!");
  }

}
