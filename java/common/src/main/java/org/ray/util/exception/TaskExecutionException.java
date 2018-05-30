package org.ray.util.exception;

/**
 * An exception which is thrown when a ray task encounters an error when executing.
 */
public class TaskExecutionException extends RuntimeException {

  public TaskExecutionException(Throwable cause) {
    super(cause);
  }

  public TaskExecutionException(String message, Throwable cause) {
    super(message, cause);
  }
}
