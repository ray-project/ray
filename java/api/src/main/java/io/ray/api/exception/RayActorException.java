package io.ray.api.exception;

import io.ray.api.id.ActorId;

/**
 * Indicates that the actor died unexpectedly before finishing a task.
 *
 * <p>This exception could happen either because the actor process dies while executing a task, or
 * because a task is submitted to a dead actor.
 *
 * <p>If the actor died because of an exception thrown in its creation tasks, RayActorError will
 * contains this exception as the cause exception.
 */
public class RayActorException extends RayException {

  public ActorId actorId;

  public RayActorException() {
    super("The actor died unexpectedly before finishing this task.");
  }

  public RayActorException(ActorId actorId) {
    super(String.format("The actor %s died unexpectedly before finishing this task.", actorId));
    this.actorId = actorId;
  }

  public RayActorException(int pid, String ipAddress, ActorId actorId, Throwable cause) {
    super(
        String.format(
            "(pid=%d, ip=%s) The actor %s died because of it's creation task failed",
            pid, ipAddress, actorId.toString()),
        cause);
    this.actorId = actorId;
  }

  public RayActorException(int pid, String ipAddress, Throwable cause) {
    super(
        String.format(
            "(pid=%d, ip=%s) The actor died because of it's creation task failed", pid, ipAddress),
        cause);
  }
}
