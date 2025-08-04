package io.ray.serve.util;

/** Ray Serve common log tool. */
public class MessageFormatter {

  public static String format(String messagePattern, Object... args) {
    return org.slf4j.helpers.MessageFormatter.arrayFormat(messagePattern, args).getMessage();
  }
}
