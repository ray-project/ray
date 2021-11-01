package io.ray.runtime.exception;

import io.ray.api.id.ActorId;

/**
 * Indicates that the back pressure occurs when submitting an actor task.
 *
 * <p>This exception could happen either because the caller calls the callee too frequently.
 */
public class BackPressureException extends RayException {

  public ActorId actorId;

  public BackPressureException(String message) {
    super("Back pressure occurs when submitting the actor call.");
  }

  public BackPressureException(ActorId actorId) {
    super(String.format("Back pressure occurs when submitting the actor call to %s.", actorId));
    this.actorId = actorId;
  }
}
