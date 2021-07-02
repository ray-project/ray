package io.ray.serve.util;

import org.slf4j.helpers.MessageFormatter;

/** Ray Serve common log tool. */
public class LogUtil {

  public static String format(String messagePattern, Object... args) {
    return MessageFormatter.arrayFormat(messagePattern, args).getMessage();
  }
}
