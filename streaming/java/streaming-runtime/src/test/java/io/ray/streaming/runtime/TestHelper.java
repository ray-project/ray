package io.ray.streaming.runtime;

public class TestHelper {

  private static volatile boolean UT_FLAG = false;

  public static void setUTFlag() {
    UT_FLAG = true;
  }

  public static void clearUTFlag() {
    UT_FLAG = false;
  }

  public static boolean isUT() {
    return UT_FLAG;
  }
}
