package io.ray.streaming.runtime.worker.context;

import com.google.common.base.MoreObjects;
import io.ray.api.RayActor;
import io.ray.api.id.ActorId;

import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.serialization.JavaSerializer;

/**
 * Job worker context of java type.
 */
public class JavaJobWorkerContext extends JobWorkerContext {

  /**
   * Worker actor's id.
   */
  private ActorId workerId;

  /**
   * JobMaster actor.
   */
  private RayActor<JobMaster> master;

  /**
   * Worker's vertex info.
   */
  private ExecutionVertex executionVertex;

  public JavaJobWorkerContext(
      ActorId workerId,
      RayActor<JobMaster> master,
      ExecutionVertex executionVertex) {
    this.workerId = workerId;
    this.master = master;
    this.executionVertex = executionVertex;
  }

  public ActorId getWorkerActorId() {
    return workerId;
  }

  public String getWorkerName() {
    return executionVertex.getVertexName();
  }

  public RayActor<JobMaster> getMaster() {
    return master;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("workerId", workerId)
      .toString();
  }

  @Override
  public byte[] getContextBytes() {
    return new JavaSerializer().serialize(this);
  }
}
