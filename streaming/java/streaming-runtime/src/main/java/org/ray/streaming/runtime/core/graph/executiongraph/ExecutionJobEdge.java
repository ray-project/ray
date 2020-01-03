package org.ray.streaming.runtime.core.graph.executiongraph;

import com.google.common.base.MoreObjects;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.jobgraph.JobEdge;

/**
 * Edge to attach execution job vertex.
 */
public class ExecutionJobEdge {

  private final ExecutionJobVertex producer;
  private final ExecutionJobVertex consumer;
  private final JobEdge jobEdge;
  private final String executionJobEdgeIndex;

  public ExecutionJobEdge(ExecutionJobVertex producer, ExecutionJobVertex consumer,
      JobEdge jobEdge) {
    this.producer = producer;
    this.consumer = consumer;
    this.jobEdge = jobEdge;
    this.executionJobEdgeIndex = generateExecutionJobEdgeIndex();
  }

  private String generateExecutionJobEdgeIndex() {
    return producer.getJobVertexId() + "—" + consumer.getJobVertexId();
  }

  public ExecutionJobVertex getProducer() {
    return producer;
  }

  public ExecutionJobVertex getConsumer() {
    return consumer;
  }

  public Partition getPartition() {
    return jobEdge.getPartition();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("producer", producer)
        .add("consumer", consumer)
        .add("jobEdge", jobEdge)
        .toString();
  }
}
