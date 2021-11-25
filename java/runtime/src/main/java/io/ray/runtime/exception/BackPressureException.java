package io.ray.runtime.exception;

import io.ray.api.id.ActorId;

/**
 * Indicates that the back pressure occurs when submitting an actor task.
 *
 * <p>This exception could happen probably because the caller calls the callee too frequently.
 */
public class BackPressureException extends RayException {

  public ActorId actorId;

  public BackPressureException(String message) {
    super(message);
  }
}
