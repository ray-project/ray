package org.ray.runtime.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(WorkerUncaughtExceptionHandler.class);

  @Override
  public void uncaughtException(Thread thread, Throwable ex) {
    LOGGER.error("Uncaught exception in thread {}: {}", thread, ex);
  }
}
