package io.ray.runtime.exception;

import io.ray.api.id.ActorId;
import org.apache.commons.lang3.exception.ExceptionUtils;

/** Indicates that the actor died unexpectedly because an exception thrown in creation task. */
public class RayActorCreationTaskException extends RayActorException {
  private static final String DEFAULT_MESSAGE =
      "The actor died unexpectedly because an exception thrown in creation task.";

  public RayActorCreationTaskException() {
    super(DEFAULT_MESSAGE);
  }

  public RayActorCreationTaskException(Throwable cause) {
    super(DEFAULT_MESSAGE, cause);
  }

  public RayActorCreationTaskException(ActorId actorId) {
    super(
        String.format(
            "The actor %s died unexpectedly because an exception thrown in creation task.",
            actorId));
    this.actorId = actorId;
  }

  public RayActorCreationTaskException(String message) {
    super(message);
  }

  public RayActorCreationTaskException(String message, Throwable cause) {
    super(message, cause);
  }

  public String serializeToString() {
    return ExceptionUtils.getStackTrace(this);
  }
}
