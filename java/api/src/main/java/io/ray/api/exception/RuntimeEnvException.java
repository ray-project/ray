package io.ray.api.exception;

public class RuntimeEnvException extends RayException {

  public RuntimeEnvException(String message) {
    super(message);
  }

  public RuntimeEnvException(String message, Throwable cause) {
    super(message, cause);
  }
}
