package io.ray.runtime.exception;

/** Indicate that there are some thing have timed out, including `Ray.get()` or others. */
public class RayTimeoutException extends RayException {
  public RayTimeoutException(String message) {
    super(message);
  }

  public RayTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
