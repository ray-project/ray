package io.ray.runtime.serialization;

/** Base class of all serde exceptions. */
public class SerdeException extends RuntimeException {

  public SerdeException(String message) {
    super(message);
  }

  public SerdeException(String message, Throwable cause) {
    super(message, cause);
  }
}
