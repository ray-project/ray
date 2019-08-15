package org.ray.api.exception;

/**
 * Base class of all ray exceptions.
 */
public class RayException extends RuntimeException {

  public RayException(String message) {
    super(message);
  }

  public RayException(String message, Throwable cause) {
    super(message, cause);
  }
}
