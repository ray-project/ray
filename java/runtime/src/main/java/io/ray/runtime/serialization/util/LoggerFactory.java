package io.ray.runtime.serialization.util;

import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

public class LoggerFactory {
  private static boolean disableLogging = true;

  public static void disableLogging() {
    disableLogging = true;
  }

  public static void enableLogging() {
    disableLogging = false;
  }

  public static Logger getLogger(Class<?> clazz) {
    if (disableLogging) {
      return NOPLogger.NOP_LOGGER;
    } else {
      return org.slf4j.LoggerFactory.getLogger(clazz);
    }
  }
}
