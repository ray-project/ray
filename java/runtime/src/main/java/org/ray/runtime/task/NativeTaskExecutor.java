package org.ray.runtime.task;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Checkpointable;
import org.ray.api.Checkpointable.Checkpoint;
import org.ray.api.Checkpointable.CheckpointContext;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.runtime.AbstractRayRuntime;

/**
 * Task executor for cluster mode.
 */
public class NativeTaskExecutor extends TaskExecutor {

  // TODO(hchen): Use the C++ config.
  private static final int NUM_ACTOR_CHECKPOINTS_TO_KEEP = 20;

  /**
   * The native pointer of core worker.
   */
  private final long nativeCoreWorkerPointer;

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

  public NativeTaskExecutor(long nativeCoreWorkerPointer, AbstractRayRuntime runtime) {
    super(runtime);
    this.nativeCoreWorkerPointer = nativeCoreWorkerPointer;
  }

  @Override
  protected void maybeSaveCheckpoint(Object actor, ActorId actorId) {
    if (!(actor instanceof Checkpointable)) {
      return;
    }
    CheckpointContext checkpointContext = new CheckpointContext(actorId,
        ++numTasksSinceLastCheckpoint, System.currentTimeMillis() - lastCheckpointTimestamp);
    Checkpointable checkpointable = (Checkpointable) actor;
    if (!checkpointable.shouldCheckpoint(checkpointContext)) {
      return;
    }
    numTasksSinceLastCheckpoint = 0;
    lastCheckpointTimestamp = System.currentTimeMillis();
    UniqueId checkpointId = new UniqueId(nativePrepareCheckpoint(nativeCoreWorkerPointer));
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
    numTasksSinceLastCheckpoint = 0;
    lastCheckpointTimestamp = System.currentTimeMillis();
    checkpointIds = new ArrayList<>();
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

      nativeNotifyActorResumedFromCheckpoint(nativeCoreWorkerPointer, checkpointId.getBytes());
    }
  }

  private static native byte[] nativePrepareCheckpoint(long nativeCoreWorkerPointer);

  private static native void nativeNotifyActorResumedFromCheckpoint(long nativeCoreWorkerPointer,
      byte[] checkpointId);
}
