package io.ray.api.exception;

public class RayException extends RuntimeException {

  public RayException(String message) {
    super(message);
  }

  public RayException(String message, Throwable cause) {
    super(message, cause);
  }
}
