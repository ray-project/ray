package io.ray.api.exception;

import io.ray.api.id.ActorId;

/**
 * Indicates that the back pressure occurs when submitting an actor task.
 *
 * <p>This exception could happen probably because the caller calls the callee too frequently.
 */
public class PendingCallsLimitExceededException extends RayException {

  public ActorId actorId;

  public PendingCallsLimitExceededException(String message) {
    super(message);
  }
}
