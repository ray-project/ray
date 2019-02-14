package org.ray.api.exception;

/**
 * Indicates that the actor died unexpectedly before finish executing a task.
 */
public class RayActorException extends RayException {

  public static final RayActorException INSTANCE = new RayActorException();

  private RayActorException() {
    super("The actor died unexpectedly before finish executing this task.");
  }
}
