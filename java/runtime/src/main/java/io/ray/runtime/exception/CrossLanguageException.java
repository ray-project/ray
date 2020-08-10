package io.ray.runtime.exception;

import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.generated.Common.RayException;

public class CrossLanguageException extends NativeRayException {

  private Language language;

  public CrossLanguageException(RayException exception) {
    super(exception.getFormattedExceptionString());
    this.language = exception.getLanguage();
  }

  public Language getLanguage() {
    return this.language;
  }
}
