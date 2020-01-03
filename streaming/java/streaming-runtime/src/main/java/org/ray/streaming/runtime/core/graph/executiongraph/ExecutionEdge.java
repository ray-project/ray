package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.ray.streaming.api.partition.Partition;

/**
 * Edge to attach execution vertex.
 */
public class ExecutionEdge implements Serializable {

  private final ExecutionVertex producer;
  private final ExecutionVertex consumer;
  private final ExecutionJobEdge executionJobEdge;
  private final String executionEdgeIndex;

  public ExecutionEdge(ExecutionVertex producer, ExecutionVertex consumer,
      ExecutionJobEdge executionJobEdge) {
    this.producer = producer;
    this.consumer = consumer;
    this.executionJobEdge = executionJobEdge;
    this.executionEdgeIndex = generateExecutionEdgeIndex();
  }

  private String generateExecutionEdgeIndex() {
    return producer.getVertexId() + "—" + consumer.getVertexId();
  }

  public ExecutionVertex getProducer() {
    return producer;
  }

  public ExecutionVertex getConsumer() {
    return consumer;
  }

  public int getProducerId() {
    return producer.getVertexId();
  }

  public int getConsumerId() {
    return consumer.getVertexId();
  }

  public String getExecutionEdgeIndex() {
    return executionEdgeIndex;
  }

  public ExecutionJobEdge getExecutionJobEdge() {
    return executionJobEdge;
  }

  public Partition getPartition() {
    return executionJobEdge.getPartition();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("producer", producer)
        .add("consumer", consumer)
        .add("executionJobEdge", executionJobEdge)
        .add("executionEdgeIndex", executionEdgeIndex)
        .toString();
  }
}