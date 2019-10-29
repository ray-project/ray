package org.ray.streaming.core.graph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.ray.streaming.core.processor.StreamProcessor;
import org.ray.streaming.plan.VertexType;

/**
 * A node in the physical execution graph.
 */
public class ExecutionNode implements Serializable {

  private int nodeId;
  private int parallelism;
  private NodeType nodeType;
  private StreamProcessor streamProcessor;
  private List<ExecutionTask> executionTaskList;
  private List<ExecutionEdge> executionEdgeList;

  public ExecutionNode(int nodeId, int parallelism) {
    this.nodeId = nodeId;
    this.parallelism = parallelism;
    this.executionTaskList = new ArrayList<>();
    this.executionEdgeList = new ArrayList<>();
  }

  public int getNodeId() {
    return nodeId;
  }

  public void setNodeId(int nodeId) {
    this.nodeId = nodeId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public List<ExecutionTask> getExecutionTaskList() {
    return executionTaskList;
  }

  public void setExecutionTaskList(List<ExecutionTask> executionTaskList) {
    this.executionTaskList = executionTaskList;
  }

  public List<ExecutionEdge> getExecutionEdgeList() {
    return executionEdgeList;
  }

  public void setExecutionEdgeList(List<ExecutionEdge> executionEdgeList) {
    this.executionEdgeList = executionEdgeList;
  }

  public void addExecutionEdge(ExecutionEdge executionEdge) {
    this.executionEdgeList.add(executionEdge);
  }

  public StreamProcessor getStreamProcessor() {
    return streamProcessor;
  }

  public void setStreamProcessor(StreamProcessor streamProcessor) {
    this.streamProcessor = streamProcessor;
  }

  public NodeType getNodeType() {
    return nodeType;
  }

  public void setNodeType(VertexType vertexType) {
    switch (vertexType) {
      case MASTER:
        this.nodeType = NodeType.MASTER;
        break;
      case SOURCE:
        this.nodeType = NodeType.SOURCE;
        break;
      case SINK:
        this.nodeType = NodeType.SINK;
        break;
      default:
        this.nodeType = NodeType.PROCESS;
    }
  }

  public enum NodeType {
    MASTER,
    SOURCE,
    PROCESS,
    SINK,
  }
}
