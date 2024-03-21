package io.ray.api.exception;

public class ActorUnavailableException extends RayException {

  public ActorUnavailableException(String message) {
    super(message);
  }

  public ActorUnavailableException(String message, Throwable cause) {
    super(message, cause);
  }
}
