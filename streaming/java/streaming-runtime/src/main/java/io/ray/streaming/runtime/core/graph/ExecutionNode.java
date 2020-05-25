package io.ray.streaming.runtime.core.graph;

import io.ray.streaming.api.Language;
import io.ray.streaming.jobgraph.VertexType;
import io.ray.streaming.operator.StreamOperator;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A node in the physical execution graph.
 */
public class ExecutionNode implements Serializable {
  private int nodeId;
  private int parallelism;
  private NodeType nodeType;
  private StreamOperator streamOperator;
  private List<ExecutionTask> executionTasks;
  private List<ExecutionEdge> inputsEdges;
  private List<ExecutionEdge> outputEdges;

  public ExecutionNode(int nodeId, int parallelism) {
    this.nodeId = nodeId;
    this.parallelism = parallelism;
    this.executionTasks = new ArrayList<>();
    this.inputsEdges = new ArrayList<>();
    this.outputEdges = new ArrayList<>();
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

  public List<ExecutionTask> getExecutionTasks() {
    return executionTasks;
  }

  public void setExecutionTasks(List<ExecutionTask> executionTasks) {
    this.executionTasks = executionTasks;
  }

  public List<ExecutionEdge> getOutputEdges() {
    return outputEdges;
  }

  public void setOutputEdges(List<ExecutionEdge> outputEdges) {
    this.outputEdges = outputEdges;
  }

  public void addOutputEdge(ExecutionEdge executionEdge) {
    this.outputEdges.add(executionEdge);
  }

  public void addInputEdge(ExecutionEdge executionEdge) {
    this.inputsEdges.add(executionEdge);
  }

  public List<ExecutionEdge> getInputsEdges() {
    return inputsEdges;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public void setStreamOperator(StreamOperator streamOperator) {
    this.streamOperator = streamOperator;
  }

  public Language getLanguage() {
    return streamOperator.getLanguage();
  }

  public NodeType getNodeType() {
    return nodeType;
  }

  public void setNodeType(VertexType vertexType) {
    switch (vertexType) {
      case SOURCE:
        this.nodeType = NodeType.SOURCE;
        break;
      case SINK:
        this.nodeType = NodeType.SINK;
        break;
      default:
        this.nodeType = NodeType.TRANSFORM;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ExecutionNode{");
    sb.append("nodeId=").append(nodeId);
    sb.append(", parallelism=").append(parallelism);
    sb.append(", nodeType=").append(nodeType);
    sb.append(", streamOperator=").append(streamOperator);
    sb.append('}');
    return sb.toString();
  }

  public enum NodeType {
    SOURCE,
    TRANSFORM,
    SINK,
  }
}
