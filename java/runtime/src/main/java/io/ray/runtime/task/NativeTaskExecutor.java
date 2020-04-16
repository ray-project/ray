package io.ray.runtime.task;

import com.google.common.base.Preconditions;
import io.ray.api.Checkpointable;
import io.ray.api.Checkpointable.Checkpoint;
import io.ray.api.Checkpointable.CheckpointContext;
import io.ray.api.id.ActorId;
import io.ray.api.id.UniqueId;
import io.ray.runtime.RayRuntimeInternal;
import java.util.ArrayList;
import java.util.List;

/**
 * Task executor for cluster mode.
 */
public class NativeTaskExecutor extends TaskExecutor<NativeTaskExecutor.NativeActorContext> {

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  static class NativeActorContext extends TaskExecutor.ActorContext {

    /**
     * Number of tasks executed since last actor checkpoint.
     */
    private int numTasksSinceLastCheckpoint = 0;

    /**
     * IDs of this actor's previous checkpoints.
     */
    private List<UniqueId> checkpointIds;

    /**
     * Timestamp of the last actor checkpoint.
     */
    private long lastCheckpointTimestamp = 0;
  }

  public NativeTaskExecutor(RayRuntimeInternal runtime) {
    super(runtime);
  }

  @Override
  protected NativeActorContext createActorContext() {
    return new NativeActorContext();
  }

  @Override
  protected void maybeSaveCheckpoint(Object actor, ActorId actorId) {
    if (!(actor instanceof Checkpointable)) {
      return;
    }
    NativeActorContext actorContext = getActorContext();
    CheckpointContext checkpointContext = new CheckpointContext(actorId,
        ++actorContext.numTasksSinceLastCheckpoint,
        System.currentTimeMillis() - actorContext.lastCheckpointTimestamp);
    Checkpointable checkpointable = (Checkpointable) actor;
    if (!checkpointable.shouldCheckpoint(checkpointContext)) {
      return;
    }
    actorContext.numTasksSinceLastCheckpoint = 0;
    actorContext.lastCheckpointTimestamp = System.currentTimeMillis();
    UniqueId checkpointId = new UniqueId(nativePrepareCheckpoint());
    List<UniqueId> checkpointIds = actorContext.checkpointIds;
    checkpointIds.add(checkpointId);
    if (checkpointIds.size() > NUM_ACTOR_CHECKPOINTS_TO_KEEP) {
      ((Checkpointable) actor).checkpointExpired(actorId, checkpointIds.get(0));
      checkpointIds.remove(0);
    }
    checkpointable.saveCheckpoint(actorId, checkpointId);
  }

  @Override
  protected void maybeLoadCheckpoint(Object actor, ActorId actorId) {
    if (!(actor instanceof Checkpointable)) {
      return;
    }
    NativeActorContext actorContext = getActorContext();
    actorContext.numTasksSinceLastCheckpoint = 0;
    actorContext.lastCheckpointTimestamp = System.currentTimeMillis();
    actorContext.checkpointIds = new ArrayList<>();
    List<Checkpoint> availableCheckpoints
        = runtime.getGcsClient().getCheckpointsForActor(actorId);
    if (availableCheckpoints.isEmpty()) {
      return;
    }
    UniqueId checkpointId = ((Checkpointable) actor).loadCheckpoint(actorId, availableCheckpoints);
    if (checkpointId != null) {
      boolean checkpointValid = false;
      for (Checkpoint checkpoint : availableCheckpoints) {
        if (checkpoint.checkpointId.equals(checkpointId)) {
          checkpointValid = true;
          break;
        }
      }
      Preconditions.checkArgument(checkpointValid,
          "'loadCheckpoint' must return a checkpoint ID that exists in the "
              + "'availableCheckpoints' list, or null.");
      nativeNotifyActorResumedFromCheckpoint(checkpointId.getBytes());
    }
  }

  private static native byte[] nativePrepareCheckpoint();

  private static native void nativeNotifyActorResumedFromCheckpoint(byte[] checkpointId);
}
