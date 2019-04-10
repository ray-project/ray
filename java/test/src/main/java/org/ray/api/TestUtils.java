package org.ray.api;

import java.util.function.Supplier;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.config.RunMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;

public class TestUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  private static final int WAIT_INTERVAL_MS = 5;

  public static void skipTestUnderSingleProcess() {
    AbstractRayRuntime runtime = (AbstractRayRuntime)Ray.internal();
    if (runtime.getRayConfig().runMode == RunMode.SINGLE_PROCESS) {
      throw new SkipException("This test doesn't work under single-process mode.");
    }
  }

  public static boolean waitForCondition(Supplier<Boolean> condition, int timeoutMillis) {
    boolean result = false;
    int waitTime = 0;
    while (true) {
      result = condition.get();
      if (result) {
        break;
      }

      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
        waitTime += WAIT_INTERVAL_MS;
        if (waitTime > timeoutMillis) {
          break;
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Got exception when sleeping: ", e);
      }
    }
    return result;
  }
}
