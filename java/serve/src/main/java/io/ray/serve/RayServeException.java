package io.ray.serve;

import io.ray.runtime.exception.RayException;

public class RayServeException extends RayException {

  private static final long serialVersionUID = 4673951342965950469L;

  public RayServeException(String message) {
    super(message);
  }

  public RayServeException(String message, Throwable cause) {
    super(message, cause);
  }
}
