package io.ray.api.exception;

/** The exception represents that there is an intentional system exit. */
public class RayIntentionalSystemExitException extends RuntimeException {

  public RayIntentionalSystemExitException(String message) {
    super(message);
  }

  public RayIntentionalSystemExitException(String message, Throwable cause) {
    super(message, cause);
  }
}
