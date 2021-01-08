package io.ray.runtime.util;

import io.ray.api.id.ActorId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.TaskId;

/**
 * Helper method for different Ids. Note: any changes to these methods must be synced with C++
 * helper functions in src/ray/common/id.h
 */
public class IdUtil {

  /**
   * Compute the actor ID of the task which created this object.
   *
   * <p>Returns The actor ID of the task which created this object.
   */
  public static ActorId getActorIdFromObjectId(ObjectId objectId) {
    byte[] taskIdBytes = new byte[TaskId.LENGTH];
    System.arraycopy(objectId.getBytes(), 0, taskIdBytes, 0, TaskId.LENGTH);
    TaskId taskId = TaskId.fromBytes(taskIdBytes);
    byte[] actorIdBytes = new byte[ActorId.LENGTH];
    System.arraycopy(
        taskId.getBytes(), TaskId.UNIQUE_BYTES_LENGTH, actorIdBytes, 0, ActorId.LENGTH);
    return ActorId.fromBytes(actorIdBytes);
  }
}
