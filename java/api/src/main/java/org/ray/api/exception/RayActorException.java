package org.ray.api.exception;

/**
 * Indicates that the actor died unexpectedly before finishing a task.
 */
public class RayActorException extends RayException {

  public static final RayActorException INSTANCE = new RayActorException();

  private RayActorException() {
    super("The actor died unexpectedly before finishing this task.");
  }
}
