package com.ray.streaming.plan;

import com.ray.streaming.operator.StreamOperator;
import java.io.Serializable;

/**
 * PlanVertex is a cell node where logic is executed.
 */
public class PlanVertex implements Serializable {

  private int vertexId;
  private int parallelism;
  private VertexType vertexType;
  private StreamOperator streamOperator;

  public PlanVertex(int vertexId, int parallelism, VertexType vertexType,
      StreamOperator streamOperator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.vertexType = vertexType;
    this.streamOperator = streamOperator;
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  @Override
  public String toString() {
    return "vertexId:" + vertexId + ",op:" + streamOperator.getClass().getSimpleName().toString()
        + ",parallelism:" + parallelism;
  }
}
