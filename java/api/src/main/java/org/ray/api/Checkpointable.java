package org.ray.api;

import java.util.List;

import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;

public interface Checkpointable {

  class CheckpointContext {

    /**
     * Actor's ID.
     */
    public final ActorId actorId;
    /**
     * Number of tasks executed since last checkpoint.
     */
    public final int numTasksSinceLastCheckpoint;
    /**
     * Time elapsed since last checkpoint, in milliseconds.
     */
    public final long timeElapsedMsSinceLastCheckpoint;

    public CheckpointContext(ActorId actorId, int numTasksSinceLastCheckpoint,
                             long timeElapsedMsSinceLastCheckpoint) {
      this.actorId = actorId;
      this.numTasksSinceLastCheckpoint = numTasksSinceLastCheckpoint;
      this.timeElapsedMsSinceLastCheckpoint = timeElapsedMsSinceLastCheckpoint;
    }
  }

  class Checkpoint {

    /**
     * Checkpoint's ID.
     */
    public final UniqueId checkpointId;
    /**
     * Checkpoint's timestamp.
     */
    public final long timestamp;

    public Checkpoint(UniqueId checkpointId, long timestamp) {
      this.checkpointId = checkpointId;
      this.timestamp = timestamp;
    }
  }

  /**
   * Whether this actor needs to be checkpointed.
   *
   * This method will be called after every task. You should implement this callback to decide
   * whether this actor needs to be checkpointed at this time, based on the checkpoint context, or
   * any other factors.
   *
   * @param checkpointContext An object that contains info about last checkpoint.
   * @return A boolean value that indicates whether this actor needs to be checkpointed.
   */
  boolean shouldCheckpoint(CheckpointContext checkpointContext);

  /**
   * Save a checkpoint to persistent storage.
   *
   * If `shouldCheckpoint` returns true, this method will be called. You should implement this
   * callback to save actor's checkpoint and the given checkpoint id to persistent storage.
   *
   * @param actorId Actor's ID.
   * @param checkpointId An ID that represents this actor's current state in GCS. You should
   *     save this checkpoint ID together with actor's checkpoint data.
   */
  void saveCheckpoint(ActorId actorId, UniqueId checkpointId);

  /**
   * Load actor's previous checkpoint, and restore actor's state.
   *
   * This method will be called when an actor is reconstructed, after actor's constructor. If the
   * actor needs to restore from previous checkpoint, this function should restore actor's state and
   * return the checkpoint ID. Otherwise, it should do nothing and return null.
   *
   * @param actorId Actor's ID.
   * @param availableCheckpoints A list of available checkpoint IDs and their timestamps, sorted
   *     by timestamp in descending order. Note, this method must return the ID of one checkpoint in
   *     this list, or null. Otherwise, an exception will be thrown.
   * @return The ID of the checkpoint from which the actor was resumed, or null if the actor should
   *     restart from the beginning.
   */
  UniqueId loadCheckpoint(ActorId actorId, List<Checkpoint> availableCheckpoints);

  /**
   * Delete an expired checkpoint;
   *
   * This method will be called when an checkpoint is expired. You should implement this method to
   * delete your application checkpoint data. Note, the maximum number of checkpoints kept in the
   * backend can be configured at `RayConfig.num_actor_checkpoints_to_keep`.
   *
   * @param actorId ID of the actor.
   * @param checkpointId ID of the checkpoint that has expired.
   */
  void checkpointExpired(ActorId actorId, UniqueId checkpointId);
}
