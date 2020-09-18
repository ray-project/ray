package io.ray.runtime.exception;

import io.ray.api.id.ActorId;

/**
 * Indicates that the actor died unexpectedly before finishing a task.
 *
 * This exception could happen either because the actor process dies while executing a task, or
 * because a task is submitted to a dead actor.
 */
public class RayActorException extends RayException {

  public RayActorException() {
    super("The actor died unexpectedly before finishing this task.");
  }

  public RayActorException(ActorId actorId) {
    super("The actor " + actorId + " died unexpectedly before finishing this task.");
  }

  public RayActorException(String message) {
    super(message);
  }

  public RayActorException(String message, Throwable cause) {
    super(message, cause);
  }

}
