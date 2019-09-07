package org.ray.api;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.function.Supplier;
import org.ray.api.annotation.RayRemote;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.config.RunMode;
import org.testng.Assert;
import org.testng.SkipException;

public class TestUtils {

  public static class LargeObject implements Serializable {

    public byte[] data = new byte[1024 * 1024];
  }

  private static final int WAIT_INTERVAL_MS = 5;

  public static void skipTestUnderSingleProcess() {
    if (getRuntime().getRayConfig().runMode == RunMode.SINGLE_PROCESS) {
      throw new SkipException("This test doesn't work under single-process mode.");
    }
  }

  public static void skipTestIfDirectActorCallEnabled() {
    if (ActorCreationOptions.DEFAULT_USE_DIRECT_CALL) {
      throw new SkipException("This test doesn't work when direct actor call is enabled.");
    }
  }

  /**
   * Wait until the given condition is met.
   *
   * @param condition A function that predicts the condition.
   * @param timeoutMs Timeout in milliseconds.
   * @return True if the condition is met within the timeout, false otherwise.
   */
  public static boolean waitForCondition(Supplier<Boolean> condition, int timeoutMs) {
    int waitTime = 0;
    while (true) {
      if (condition.get()) {
        return true;
      }

      try {
        java.util.concurrent.TimeUnit.MILLISECONDS.sleep(WAIT_INTERVAL_MS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      waitTime += WAIT_INTERVAL_MS;
      if (waitTime > timeoutMs) {
        break;
      }
    }
    return false;
  }

  @RayRemote
  private static String hi() {
    return "hi";
  }

  /**
   * Warm up the cluster to make sure there's at least one idle worker.
   *
   * This is needed before calling `wait`. Because, in Travis CI, starting a new worker
   * process could be slower than the wait timeout.
   * TODO(hchen): We should consider supporting always reversing a certain number of
   * idle workers in Raylet's worker pool.
   */
  public static void warmUpCluster() {
    RayObject<String> obj = Ray.call(TestUtils::hi);
    Assert.assertEquals(obj.get(), "hi");
  }

  public static AbstractRayRuntime getRuntime() {
    RayRuntime runtime = Ray.internal();
    if (runtime instanceof RayMultiWorkerNativeRuntime) {
      runtime = ((RayMultiWorkerNativeRuntime) runtime).getCurrentRuntime();
    }
    Preconditions.checkState(runtime instanceof AbstractRayRuntime);
    return (AbstractRayRuntime) runtime;
  }
}
