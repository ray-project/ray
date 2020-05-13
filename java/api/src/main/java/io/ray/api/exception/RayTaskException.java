package io.ray.api.exception;

/**
 * Indicates that a task threw an exception during execution.
 *
 * If a task throws an exception during execution, a RayTaskException is stored in the object store
 * as the task's output. Then when the object is retrieved from the object store, this exception
 * will be thrown and propagate the error message.
 */
public class RayTaskException extends RayException {

  public RayTaskException(String message, Throwable cause) {
    super(message, cause);
  }
}
