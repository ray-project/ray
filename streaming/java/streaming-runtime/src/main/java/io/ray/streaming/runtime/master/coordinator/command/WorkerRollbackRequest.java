package io.ray.streaming.runtime.master.coordinator.command;

import com.google.common.base.MoreObjects;
import io.ray.api.id.ActorId;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;

public final class WorkerRollbackRequest extends BaseWorkerCmd {

  public static String DEFAULT_PID = "UNKNOWN_PID";
  public Long cascadingGroupId = null;
  public boolean isForcedRollback = false;
  private String exceptionMsg = "No detail message.";
  private String hostname = "UNKNOWN_HOST";
  private String pid = DEFAULT_PID;

  public WorkerRollbackRequest(ActorId actorId) {
    super(actorId);
  }

  public WorkerRollbackRequest(ActorId actorId, String msg) {
    super(actorId);
    exceptionMsg = msg;
  }

  public WorkerRollbackRequest(
      ExecutionVertex executionVertex, String hostname, String msg, boolean isForcedRollback) {

    super(executionVertex.getWorkerActorId());

    this.hostname = hostname;
    this.pid = executionVertex.getPid();
    this.exceptionMsg = msg;
    this.isForcedRollback = isForcedRollback;
  }

  public WorkerRollbackRequest(ActorId actorId, String msg, String hostname, String pid) {
    this(actorId, msg);
    this.hostname = hostname;
    this.pid = pid;
  }

  public String getRollbackExceptionMsg() {
    return exceptionMsg;
  }

  public String getHostname() {
    return hostname;
  }

  public String getPid() {
    return pid;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("fromActorId", fromActorId).toString();
  }
}
