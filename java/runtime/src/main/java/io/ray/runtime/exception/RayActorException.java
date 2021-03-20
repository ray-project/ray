package io.ray.runtime.exception;

import io.ray.api.id.ActorId;
import io.ray.runtime.util.NetworkUtil;
import io.ray.runtime.util.SystemUtil;

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

  public RayActorException(ActorId actorId, Throwable cause) {
    super(
        String.format(
            "(pid=%d, ip=%s) The actor %s died because of it's creation task failed",
            SystemUtil.pid(), NetworkUtil.getIpAddress(null), actorId.toString()),
        cause);
    this.actorId = actorId;
  }

  public RayActorException(Throwable cause) {
    super(
        String.format(
            "(pid=%d, ip=%s) The actor died because of it's creation task failed",
            SystemUtil.pid(), NetworkUtil.getIpAddress(null)),
        cause);
  }
}
