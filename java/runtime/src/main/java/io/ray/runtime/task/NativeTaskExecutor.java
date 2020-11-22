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

  public void onWorkerShutdown(byte[] workerIdBytes) {
    // This is to make sure no memory leak when `Ray.exitActor()` is called.
    removeActorContext(new UniqueId(workerIdBytes));
  }
}
