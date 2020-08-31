package io.ray.streaming.runtime.master.coordinator.command;

import com.google.common.base.MoreObjects;
import io.ray.api.id.ActorId;

public final class WorkerCommitReport extends BaseWorkerCmd {

  public final long commitCheckpointId;

  public WorkerCommitReport(ActorId actorId, long commitCheckpointId) {
    super(actorId);
    this.commitCheckpointId = commitCheckpointId;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("commitCheckpointId", commitCheckpointId)
        .add("fromActorId", fromActorId)
        .toString();
  }
}
