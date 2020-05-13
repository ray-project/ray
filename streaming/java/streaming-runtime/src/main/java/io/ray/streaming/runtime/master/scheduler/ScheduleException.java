package io.ray.streaming.runtime.master.scheduler;

public class ScheduleException extends RuntimeException {

  public ScheduleException() {
    super();
  }

  public ScheduleException(String message) {
    super(message);
  }

  public ScheduleException(String message, Throwable cause) {
    super(message, cause);
  }

  public ScheduleException(Throwable cause) {
    super(cause);
  }

  protected ScheduleException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
