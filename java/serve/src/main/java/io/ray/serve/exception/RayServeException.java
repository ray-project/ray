package io.ray.serve.exception;

import io.ray.api.exception.RayException;

public class RayServeException extends RayException {

  private static final long serialVersionUID = 4673951342965950469L;

  public RayServeException(String message) {
    super(message);
  }

  public RayServeException(String message, Throwable cause) {
    super(message, cause);
  }
}
