package io.ray.performancetest;

public class TestUtils {
  private static boolean DEV_MODE = false;

  static {
    if (null != System.getenv("PERF_TEST_DEV")) {
      DEV_MODE = true;
    }
  }

  public static boolean isDevMode() {
    return DEV_MODE;
  }
}
