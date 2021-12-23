package io.ray.serialization;

/** Base class of all fury exceptions. */
public class FuryException extends RuntimeException {

  public FuryException(String message) {
    super(message);
  }

  public FuryException(String message, Throwable cause) {
    super(message, cause);
  }
}
