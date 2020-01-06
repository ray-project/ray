package org.ray.streaming.runtime;

public class TestHelper {

  private static final String UT_PATTERN = "UT_PATTERN";

  public static void setUTPattern() {
    System.setProperty(UT_PATTERN, "");
  }

  public static void clearUTPattern() {
    System.clearProperty(UT_PATTERN);
  }

  public static boolean isUTPattern() {
    return System.getProperty(UT_PATTERN) != null;
  }
}