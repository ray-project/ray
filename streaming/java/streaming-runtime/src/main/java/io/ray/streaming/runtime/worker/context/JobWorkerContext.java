package org.ray.streaming.runtime.worker.context;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.master.JobMaster;

/**
 * Job worker context.
 */
public class JobWorkerContext implements Serializable {

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

  public JobWorkerContext(
      ActorId workerId,
      RayActor<JobMaster> master,
      ExecutionVertex executionVertex) {
    this.workerId = workerId;
    this.master = master;
    this.executionVertex = executionVertex;
  }

  public ActorId getWorkerId() {
    return workerId;
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
      .add("master", master)
      .toString();
  }

}
