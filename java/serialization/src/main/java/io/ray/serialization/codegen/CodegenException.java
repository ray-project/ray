package io.ray.serialization.codegen;

import io.ray.serialization.exception.FuryException;

public class CodegenException extends FuryException {
  public CodegenException(String message) {
    super(message);
  }

  public CodegenException(String message, Throwable cause) {
    super(message, cause);
  }
}
